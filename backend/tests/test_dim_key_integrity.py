"""维度键完整性测试：事实外键、键版本和分区交付共用同一份明细。

覆盖点：
- 已停用、跨生效期、不存在、未确认的键都不能计为通过；
- 历史有效键、当前键、未知键混在一批分区里，通过率由同一批明细算出；
- 重复校验逐分区重算，不跳过跨版本边界的分区，并能核对实际改变的外键；
- 事实设计发布和部署前置检查复用同一份明细。
"""
from __future__ import annotations

import pytest

from app.store import Store
from app.services import pipeline
from app.services.dim_key_integrity import (
    DEACTIVATED,
    OUTSIDE_EFFECTIVE_PERIOD,
    UNKNOWN,
    UNVERIFIED,
    VALID,
    DimensionKeyIndex,
    evaluate_fact_design,
    evaluate_records,
    fact_dimension_fields,
    refill_missing_keys,
)
from app.services.fact_design import FactDesignService
from app.services.model_validate import ModelValidateService


@pytest.fixture
def store() -> Store:
    return Store()


@pytest.fixture
def versioned_store() -> Store:
    """一个维度跨越换键边界：旧键只在 2025 年有效，新键从 2026 年起有效。"""
    instance = Store()
    instance.rows("dim_definition").clear()
    instance.rows("dim_definition").extend([
        {
            "id": 11, "维度编码": "DIM_PRODUCT", "维度名称": "产品维度",
            "主键定义": "product_id VARCHAR(32)", "status": "已发布",
            "current_marker": "0", "生效时间": "2025-01-01", "失效时间": "2025-12-31",
            "键值": "PRD-OLD",
        },
        {
            "id": 12, "维度编码": "DIM_PRODUCT", "维度名称": "产品维度v2",
            "主键定义": "product_id VARCHAR(32)", "status": "已发布",
            "current_marker": "1", "生效时间": "2026-01-01", "失效时间": "9999-12-31",
            "键值": "PRD-NEW",
        },
        {
            "id": 13, "维度编码": "DIM_STORE", "维度名称": "门店维度",
            "主键定义": "store_id INT", "status": "已停用",
            "current_marker": "1", "生效时间": "2025-01-01", "失效时间": "9999-12-31",
            "键值": "STORE-001",
        },
        {
            "id": 14, "维度编码": "DIM_CHANNEL", "维度名称": "渠道维度",
            "主键定义": "channel_code VARCHAR(16)", "status": "草稿",
            "current_marker": "1", "生效时间": "2026-01-01", "失效时间": "9999-12-31",
            "键值": "CHN-ONLINE",
        },
    ])
    return instance


def _write(store: Store, records: list[dict], **kwargs) -> dict:
    return pipeline.fact_partition_write(
        fact_key="销售明细事实",
        records=records,
        partition_field="dt",
        dim_fk_fields={"product_id": "DIM_PRODUCT", "store_id": "DIM_STORE"},
        store=store,
        **kwargs,
    )


class TestKeyVersionClassification:
    """单个键值在不同事实日期下的可用性判定。"""

    def test_historical_key_still_valid_inside_its_period(self, versioned_store: Store) -> None:
        index = DimensionKeyIndex.from_store(versioned_store)
        status, version = index.classify("DIM_PRODUCT", pipeline.parse_fact_date("2025-06-01"), "PRD-OLD")
        assert status == VALID
        assert version is not None and version.version_id == 11

    def test_current_key_valid_after_version_boundary(self, versioned_store: Store) -> None:
        index = DimensionKeyIndex.from_store(versioned_store)
        status, version = index.classify("DIM_PRODUCT", pipeline.parse_fact_date("2026-11-01"), "PRD-NEW")
        assert status == VALID
        assert version is not None and version.version_id == 12

    def test_old_key_after_boundary_is_outside_effective_period(self, versioned_store: Store) -> None:
        """换键后仍落在旧键有效期内的历史事实，新旧标准冲突，不能计为通过。"""
        index = DimensionKeyIndex.from_store(versioned_store)
        status, _ = index.classify("DIM_PRODUCT", pipeline.parse_fact_date("2026-11-01"), "PRD-OLD")
        assert status == OUTSIDE_EFFECTIVE_PERIOD

    def test_unknown_key_is_not_counted_as_pass(self, versioned_store: Store) -> None:
        """维度定义存在但外键值不存在，不能靠“定义存在”判为通过。"""
        index = DimensionKeyIndex.from_store(versioned_store)
        status, _ = index.classify("DIM_PRODUCT", pipeline.parse_fact_date("2026-11-01"), "PRD-GHOST")
        assert status == UNKNOWN

    def test_deactivated_dimension_is_flagged(self, versioned_store: Store) -> None:
        index = DimensionKeyIndex.from_store(versioned_store)
        status, _ = index.classify("DIM_STORE", pipeline.parse_fact_date("2026-11-01"), "STORE-001")
        assert status == DEACTIVATED

    def test_unconfirmed_dimension_is_flagged(self, versioned_store: Store) -> None:
        index = DimensionKeyIndex.from_store(versioned_store)
        status, _ = index.classify("DIM_CHANNEL", pipeline.parse_fact_date("2026-11-01"), "CHN-ONLINE")
        assert status == UNVERIFIED


class TestPassRateFromSameBatch:
    """通过率必须由同一批记录算出，且历史、当前、未知键混排时不漏算。"""

    def test_mixed_batch_counts_every_key(self, versioned_store: Store) -> None:
        records = [
            {"dt": "2025-06-01", "product_id": "PRD-OLD", "store_id": "STORE-001"},
            {"dt": "2026-11-01", "product_id": "PRD-NEW", "store_id": "STORE-001"},
            {"dt": "2026-11-02", "product_id": "PRD-GHOST", "store_id": "STORE-001"},
        ]
        result = _write(versioned_store, records)

        # 6 个外键里只有 1 个真正可用：PRD-OLD@2025 和 PRD-NEW@2026。
        assert result["summary"]["total"] == 6
        assert result["pass_rate"] == "2/6"
        assert result["status_counts"][VALID] == 2
        assert result["status_counts"][DEACTIVATED] == 3
        assert result["status_counts"][UNKNOWN] == 1

    def test_each_detail_reports_fact_date_and_key_version(self, versioned_store: Store) -> None:
        records = [{"dt": "2025-06-01", "product_id": "PRD-OLD", "store_id": "STORE-001"}]
        result = _write(versioned_store, records)
        product_detail = next(d for d in result["fk_details"] if d["field"] == "product_id")
        assert product_detail["fact_date"] == "2025-06-01"
        assert product_detail["key_version"] == "DIM_PRODUCT#11"
        assert product_detail["current_key_version"] == "DIM_PRODUCT#12"
        assert product_detail["status"] == VALID

    def test_blocked_partition_keeps_source_records(self, versioned_store: Store) -> None:
        records = [
            {"dt": "2026-11-01", "product_id": "PRD-NEW", "store_id": "STORE-001"},
            {"dt": "2026-11-01", "product_id": "PRD-GHOST", "store_id": "STORE-001"},
        ]
        result = _write(versioned_store, records)
        # 同一个分区只要有脏键，整分区拦截，源记录保留待更正。
        assert result["wrote"] == 0
        assert result["delivered_partitions"] == []
        assert [b["partition"] for b in result["blocked_partitions"]] == ["2026-11-01"]
        assert len(result["corrections"]) == 2
        assert result["records"][1]["product_id"] == "PRD-GHOST"

    def test_only_clean_partitions_are_delivered(self, versioned_store: Store) -> None:
        records = [
            {"dt": "2025-06-01", "product_id": "PRD-OLD", "store_id": "STORE-001"},
            {"dt": "2026-11-01", "product_id": "PRD-NEW", "store_id": "STORE-001"},
        ]
        result = _write(versioned_store, records)
        # 两个分区都因为 DIM_STORE 停用而失败，不能交付。
        assert result["delivered_partitions"] == []
        assert result["wrote"] == 0


class TestRevalidation:
    """重复校验覆盖全部分区，能核对实际改变的外键。"""

    def test_revalidation_covers_cross_version_partitions(self, versioned_store: Store) -> None:
        batch = [
            {"dt": "2025-06-01", "product_id": "PRD-OLD", "store_id": "STORE-001"},
            {"dt": "2026-11-01", "product_id": "PRD-GHOST", "store_id": "STORE-001"},
        ]
        first = _write(versioned_store, batch)
        assert first["summary"]["total"] == 4

        # 维度键的补键不影响另一批记录：只更正脏键，重新校验同一批记录。
        corrected = [batch[0], {"dt": "2026-11-01", "product_id": "PRD-NEW", "store_id": "STORE-001"}]
        second = _write(versioned_store, corrected, previous_details=first["fk_details"])

        assert second["summary"]["total"] == 4
        assert second["fact_dates"] == ["2025-06-01", "2026-11-01"]
        # 2025-06-01 命中旧版本，和当前版本不同，说明这个分区跨过了换键边界。
        assert second["version_boundaries"] == ["2025-06-01"]
        changes = second["changed_foreign_keys"]
        assert len(changes) == 1
        assert changes[0]["old_value"] == "PRD-GHOST"
        assert changes[0]["new_value"] == "PRD-NEW"
        assert changes[0]["old_status"] == UNKNOWN
        assert changes[0]["new_status"] == VALID

    def test_auto_refill_rechecks_whole_batch(self, versioned_store: Store) -> None:
        batch = [
            {"dt": "2024-06-01", "product_id": "PRD-OLD", "store_id": "STORE-001"},
            {"dt": "2026-11-01", "product_id": "PRD-NEW", "store_id": "STORE-001"},
        ]
        result = _write(versioned_store, batch, auto_refill=True)
        # PRD-OLD 在 2024 年没有任何版本区间，补一个从事实日期开始的版本；
        # 补键后整批重算，另一分区也重新校验，不会沿用上一次结果。
        assert any(refill["dim_code"] == "DIM_PRODUCT" for refill in result["refills"])
        assert result["summary"]["total"] == 4
        assert result["fact_dates"] == ["2024-06-01", "2026-11-01"]
        old_detail = next(
            d for d in result["fk_details"]
            if d["partition"] == "2024-06-01" and d["field"] == "product_id"
        )
        assert old_detail["status"] == VALID

    def test_refill_never_hides_unknown_keys(self, versioned_store: Store) -> None:
        result = _write(
            versioned_store,
            [{"dt": "2026-11-01", "product_id": "PRD-GHOST", "store_id": "STORE-001"}],
            auto_refill=True,
        )
        # 未知键不会被补键“洗白”，停用维度也不会；问题只能靠更正源记录解决。
        assert result["refills"] == []
        assert result["pass_rate"] == "0/2"
        assert result["status_counts"][UNKNOWN] == 1
        assert result["status_counts"][DEACTIVATED] == 1


class TestFactDesignDelivery:
    """事实设计发布时按外键校验结果安排补键和交付。"""

    def test_design_reuses_one_batch_for_validation_and_delivery(self, versioned_store: Store) -> None:
        service = FactDesignService()
        # FactDesignService 使用模块级 store，这里用 monkeypatch 对齐到测试实例。
        import app.services.fact_design as fact_design_module
        original = fact_design_module.store
        fact_design_module.store = versioned_store
        try:
            entry, missing = service.create_entry({
                "事实名称": "多版本事实", "业务过程": "零售", "粒度声明": "明细",
                "关联维度": "DIM_PRODUCT", "分区字段": "dt DATE",
                "分区记录": [
                    {"dt": "2025-06-01", "product_id": "PRD-OLD"},
                    {"dt": "2026-11-01", "product_id": "PRD-GHOST"},
                ],
            })
            assert entry is not None and not missing
            assert entry["_delivery"]["pass_rate"] == "1/2"
            assert entry["_delivery"]["delivered_partitions"] == ["2025-06-01"]
            assert entry["_delivery"]["blocked_partitions"][0]["partition"] == "2026-11-01"

            # 发布时复用同一批记录重新校验，不会因为已交付过而漏掉跨版本边界的分区。
            result, message = service.run_action(entry["id"], "发布事实")
            assert result is not None
            assert result["_fk_summary"]["total"] == 2
            assert set(result["_delivery"]["key_versions"]) == {"DIM_PRODUCT#11", "DIM_PRODUCT#12"}
        finally:
            fact_design_module.store = original


class TestValidationSharesDetails:
    """模型校验的维度键完整性复用同一份明细。"""

    def test_rule_records_fact_date_and_key_versions(self, versioned_store: Store) -> None:
        import app.services.model_validate as validate_module
        original = validate_module.store
        validate_module.store = versioned_store
        try:
            service = ModelValidateService()
            entry, _ = service.create_entry({
                "规则名称": "多版本键校验", "目标模型": "DIM_PRODUCT",
                "校验类型": "维度键完整性",
                "期望结果": "事实日期=2026-11-01 全部键可关联",
            })
            assert entry is not None
            result, _ = service.run_action(entry["id"], "执行校验")
            assert result is not None
            assert result["status"] == "通过"
            assert result["_fact_dates"] == ["2026-11-01"]
            assert result["_key_versions"] == ["DIM_PRODUCT#12"]
            assert result["_version_boundaries"] == []
            assert all(detail["fact_date"] == "2026-11-01" for detail in result["_fk_details"])
        finally:
            validate_module.store = original

    def test_rule_can_fail_when_only_definition_exists(self, versioned_store: Store) -> None:
        """只确认了维度定义存在、但没有可用键版本时不能判为通过。"""
        import app.services.model_validate as validate_module
        original = validate_module.store
        validate_module.store = versioned_store
        try:
            service = ModelValidateService()
            entry, _ = service.create_entry({
                "规则名称": "未确认维度校验", "目标模型": "DIM_CHANNEL",
                "校验类型": "维度键完整性",
                "期望结果": "事实日期=2026-11-01 全部键可关联",
            })
            result, _ = service.run_action(entry["id"], "执行校验")
            assert result is not None
            assert result["status"] == "未通过"
            assert result["_fk_details"][0]["status"] == UNVERIFIED
        finally:
            validate_module.store = original
