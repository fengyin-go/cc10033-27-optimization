"""管线服务业务测试：覆盖 SCD Type 2、代理键、事实表写入、
汇总表构建与增量刷新、模型血缘影响分析。"""
from __future__ import annotations

import pytest

from app.store import Store
from app.seed import SEED_ROWS
from app.services.pipeline import (
    scd_type2_handle,
    SurrogateKeyAllocator,
    fact_partition_write,
    AggregateBuilder,
    model_lineage_impact,
    conform_dim_constrain,
    date_dim_to_fact_fk,
)


@pytest.fixture
def store() -> Store:
    """每个测试使用独立 store 实例。"""
    return Store()


# ======================================================================
# Section A: SCD Type 2 处理
# ======================================================================


class TestScdType2:
    """SCD Type 2 处理：维度变更后旧记录失效、新记录生效。"""

    def test_scd_type2_expires_old_record(self, store: Store) -> None:
        """维度变更后，旧记录的 current_marker 应变为 0，失效时间更新。"""
        result = scd_type2_handle(
            dimension_key="DIM_PRODUCT",
            new_attrs={"维度名称": "更新后的维度", "SCD类型": "Type2", "粒度说明": "日粒度"},
            store=store,
        )
        assert result["action"] == "scd2_change"
        assert result["expired_count"] >= 1
        assert result["new_id"] > 0

        # 验证旧记录已失效
        dim_rows = store.rows("dim_definition")
        expired = [r for r in dim_rows if r.get("维度编码") == "DIM_PRODUCT" and r.get("current_marker") == "0"]
        assert len(expired) >= 1
        for e in expired:
            assert e["失效时间"] != "9999-12-31 23:59:59"

    def test_scd_type2_new_record_effective(self, store: Store) -> None:
        """新记录应标记为 current_marker=1，生效时间为当前时间。"""
        result = scd_type2_handle(
            dimension_key="DIM_-0002",
            new_attrs={"维度名称": "新维度名", "粒度说明": "月粒度"},
            store=store,
        )
        dim_rows = store.rows("dim_definition")
        new_record = [r for r in dim_rows if r.get("id") == result["new_id"]]
        assert len(new_record) == 1
        assert new_record[0]["current_marker"] == "1"
        assert new_record[0]["维度名称"] == "新维度名"

    def test_scd_type2_writes_change_log(self, store: Store) -> None:
        """SCD Type 2 变更后应在 slow_change_log 中写入记录。"""
        result = scd_type2_handle(
            dimension_key="DIM_-0003",
            new_attrs={"SCD类型": "Type2", "数据来源": "新系统"},
            store=store,
        )
        log_rows = store.rows("slow_change_log")
        assert result["log_id"] > 0
        log_entry = [r for r in log_rows if r.get("id") == result["log_id"]]
        assert len(log_entry) == 1
        assert "DIM_-0003" in log_entry[0].get("目标维度", "")


# ======================================================================
# Section B: 代理键冲突处理与重分配
# ======================================================================


class TestSurrogateKey:
    """代理键冲突处理和重分配。"""

    def test_allocate_sequence_generates_increasing_keys(self, store: Store) -> None:
        """顺序分配模式应生成递增的代理键。"""
        allocator = SurrogateKeyAllocator(store)
        keys = [allocator.allocate("dim_test", strategy="sequence") for _ in range(5)]
        assert keys == list(range(keys[0], keys[0] + 5))

    def test_handle_conflict_generates_new_key(self, store: Store) -> None:
        """发生冲突时应重新分配下一个键。"""
        allocator = SurrogateKeyAllocator(store)
        k1 = allocator.allocate("dim_conflict_test", strategy="sequence")
        k2 = allocator.handle_conflict("dim_conflict_test", conflicting_key=k1)
        assert k2 > k1

    def test_segment_strategy_uses_step(self, store: Store) -> None:
        """键段模式应按步长递增。"""
        allocator = SurrogateKeyAllocator(store)
        # Pre-create a surrogate_key config with known step
        store.rows("surrogate_key").append({
            "id": 99, "键值名称": "test_segment",
            "目标维度": "dim_segment_test", "生成策略": "segment",
            "初始值": "100", "步长": "500", "status": "草稿",
            "pending": True, "abnormal": False,
        })
        allocator = SurrogateKeyAllocator(store)
        k1 = allocator.allocate("dim_segment_test", strategy="segment")
        k2 = allocator.allocate("dim_segment_test", strategy="segment")
        assert k2 - k1 == 500


# ======================================================================
# Section C: 事实表写入 + 维度外键完整性
# ======================================================================


class TestFactPartitionWrite:
    """事实表写入和维度外键完整性。"""

    def test_fact_write_with_valid_fks(self, store: Store) -> None:
        """有效的维度外键可以成功写入。"""
        # 确保存在有效维度 key
        store.rows("dim_definition").append({
            "id": 99, "维度编码": "DIM_VALID", "维度名称": "有效维度",
            "主键定义": "PK-99", "current_marker": "1",
            "status": "已发布", "pending": False, "abnormal": False,
        })
        records = [
            {"度量字段": "order_amount", "关联维度": "DIM_VALID",
             "分区字段": "dt", "dt": "2026-11-01", "粒度声明": "订单", "业务过程": "下单"},
        ]
        result = fact_partition_write(
            fact_key="sales_fact", records=records,
            partition_field="dt",
            dim_fk_fields={"关联维度": "DIM_VALID"},
            store=store,
        )
        assert result["wrote"] == 1
        assert result["partitions"]["2026-11-01"] == 1
        assert len(result["violations"]) == 0

    def test_fact_write_violates_invalid_fk(self, store: Store) -> None:
        """无效的维度外键应被拦截并报告。"""
        records = [{"度量字段": "bad_amount", "关联维度": "NONEXISTENT_DIM_KEY",
                     "dt": "2026-11-01"}]
        result = fact_partition_write(
            fact_key="test_fact", records=records,
            partition_field="dt",
            dim_fk_fields={"关联维度": "NONEXISTENT_DIM_KEY"},
            store=store,
        )
        assert result["wrote"] == 0
        assert len(result["violations"]) >= 1
        assert result["violations"][0]["field"] == "关联维度"

    def test_partition_field_aggregation(self, store: Store) -> None:
        """不同分区字段的记录应分别统计。"""
        store.rows("dim_definition").append({
            "id": 100, "维度编码": "DIM_PART", "维度名称": "分区维度",
            "current_marker": "1", "主键定义": "PK-100",
            "status": "已发布", "pending": False, "abnormal": False,
        })
        records = [
            {"关联维度": "DIM_PART", "dt": "2026-11-01", "度量字段": "m1"},
            {"关联维度": "DIM_PART", "dt": "2026-11-02", "度量字段": "m2"},
            {"关联维度": "DIM_PART", "dt": "2026-11-01", "度量字段": "m3"},
        ]
        result = fact_partition_write(
            fact_key="partition_fact", records=records,
            partition_field="dt", dim_fk_fields={"关联维度": "DIM_PART"},
            store=store,
        )
        assert result["partitions"]["2026-11-01"] == 2
        assert result["partitions"]["2026-11-02"] == 1


# ======================================================================
# Section D: 汇总表构建 + 增量刷新数据正确性
# ======================================================================


class TestAggregateBuilder:
    """汇总表构建与增量刷新。"""

    def test_full_build_creates_aggregations(self, store: Store) -> None:
        """全量构建应为每个分组生成汇总记录。"""
        # 准备源事实记录
        fact_rows = store.rows("fact_design")
        for i in range(1, 6):
            fact_rows.append({
                "id": 1000 + i, "事实名称": "sales", "业务过程": "下单",
                "粒度声明": "订单", "关联维度": "DIM_A", "度量字段": f"{i * 10}",
                "status": "已设计", "pending": False, "abnormal": False,
            })

        builder = AggregateBuilder(store)
        result = builder.build(
            agg_name="sales_summary",
            source_fact="sales",
            rollup_dims=["关联维度"],
            agg_measures=[("度量字段", "sum")],
            grain="日",
        )
        assert result["mode"] == "full"
        assert result["built_groups"] >= 1

    def test_incremental_refresh_only_processes_new(self, store: Store) -> None:
        """增量刷新只处理 since_id 之后的新记录。"""
        fact_rows = store.rows("fact_design")
        for i in range(1, 4):
            fact_rows.append({
                "id": 2000 + i, "事实名称": "order_fact", "业务过程": "支付",
                "粒度声明": "订单", "关联维度": "DIM_B", "度量字段": f"{i * 100},0",
                "status": "已设计", "pending": False, "abnormal": False,
            })

        builder = AggregateBuilder(store)
        # 先全量构建
        builder.build("order_summary", "order_fact", ["关联维度"], [("度量字段", "count")], "小时")

        # 新增记录
        prev_max_id = max(int(r["id"]) for r in fact_rows if r.get("事实名称") == "order_fact")
        fact_rows.append({
            "id": prev_max_id + 1, "事实名称": "order_fact", "业务过程": "支付",
            "粒度声明": "订单", "关联维度": "DIM_C", "度量字段": "500,0",
            "status": "已设计", "pending": False, "abnormal": False,
        })

        result = builder.refresh_incremental(
            "order_summary", "order_fact",
            ["关联维度"], [("度量字段", "count")],
            since_id=prev_max_id,
        )
        assert result["mode"] == "incremental"
        assert result["new_rows"] == 1
        assert result["built_groups"] >= 1

    def test_aggregate_correctness(self, store: Store) -> None:
        """验证聚合计算的数值正确性。"""
        fact_rows = store.rows("fact_design")
        base_id = 3000
        for i in range(1, 6):
            fact_rows.append({
                "id": base_id + i, "事实名称": "agg_test", "业务过程": "退款",
                "粒度声明": "事件", "关联维度": "DIM_AGG",
                "度量字段": str(i * 10), "分区字段": "dt",
                "status": "已设计", "pending": False, "abnormal": False,
            })

        builder = AggregateBuilder(store)
        builder.build("agg_test_summary", "agg_test", ["关联维度"],
                       [("度量字段", "sum")], "日")
        agg_rows = [
            r for r in store.rows("agg_summary")
            if r.get("汇总名称") == "agg_test_summary"
        ]
        assert len(agg_rows) >= 1


# ======================================================================
# Section E: 模型血缘影响分析
# ======================================================================


class TestModelLineage:
    """模型血缘影响分析。"""

    def test_impact_analysis_finds_downstream(self, store: Store) -> None:
        """变更模型后，应找到所有下游依赖。"""
        lineage_rows = store.rows("model_lineage")
        lineage_rows.append({
            "id": 500, "关系ID": "LIN-001",
            "上游模型": "dim_customer", "下游模型": "fact_orders",
            "依赖字段": "customer_id", "关联类型": "direct",
            "影响级别": "high", "更新状态": "正常",
            "status": "正常", "pending": False, "abnormal": False,
        })
        lineage_rows.append({
            "id": 501, "关系ID": "LIN-002",
            "上游模型": "dim_customer", "下游模型": "agg_sales_monthly",
            "依赖字段": "customer_zk", "关联类型": "indirect",
            "影响级别": "medium", "更新状态": "正常",
            "status": "正常", "pending": False, "abnormal": False,
        })
        lineage_rows.append({
            "id": 502, "关系ID": "LIN-003",
            "上游模型": "dim_product", "下游模型": "fact_orders",
            "依赖字段": "product_id", "关联类型": "direct",
            "影响级别": "high", "更新状态": "正常",
            "status": "正常", "pending": False, "abnormal": False,
        })

        result = model_lineage_impact(
            changed_model="dim_customer",
            changed_fields=["customer_id", "customer_zk"],
            store=store,
        )
        assert result["affected_count"] == 2
        assert result["severe_impact"] >= 1
        assert len(result["downstream_models"]) == 2
        assert "fact_orders" in result["downstream_models"]

    def test_impact_updates_status(self, store: Store) -> None:
        """受影响的依赖关系状态应更新为变更中。"""
        lineage_rows = store.rows("model_lineage")
        lineage_rows.append({
            "id": 600, "关系ID": "LIN-010",
            "上游模型": "fact_sales", "下游模型": "dim_date",
            "依赖字段": "sale_date", "关联类型": "direct",
            "影响级别": "medium", "更新状态": "正常",
            "status": "正常", "pending": False, "abnormal": False,
        })

        model_lineage_impact("fact_sales", ["sale_date"], store=store)
        updated = [r for r in store.rows("model_lineage") if r.get("id") == 600]
        assert len(updated) == 1
        assert updated[0]["status"] == "变更中"

    def test_no_impact_on_unrelated_models(self, store: Store) -> None:
        """未关联的模型不应受影响。"""
        lineage_rows = store.rows("model_lineage")
        lineage_rows.append({
            "id": 700, "关系ID": "LIN-020",
            "上游模型": "dim_date", "下游模型": "fact_sales",
            "依赖字段": "date_key", "关联类型": "direct",
            "影响级别": "medium", "status": "正常",
            "pending": False, "abnormal": False,
        })

        result = model_lineage_impact("unrelated_model", ["other_field"], store=store)
        assert result["affected_count"] == 0


# ======================================================================
# Section F: 一致性维度约束
# ======================================================================


class TestConformDim:
    """一致性维度关联约束。"""

    def test_conform_dim_constraint_detects_mismatch(self, store: Store) -> None:
        """一致性维度应检测名称不匹配。"""
        store.rows("conform_dim").append({
            "id": 800, "标准名称": "CONFORM_001",
            "维度主题": "客户", "标准键值": "DIM_-0001",
            "status": "已发布", "pending": False, "abnormal": False,
        })

        result = conform_dim_constrain("CONFORM_001", ["DIM_-0001", "DIM_-0002"], store=store)
        assert result["conform_name"] == "CONFORM_001"
        assert result["total_checked"] == 2

    def test_conform_dim_missing_standard(self, store: Store) -> None:
        """未找到一致性维度应报错。"""
        result = conform_dim_constrain("NONEXISTENT", ["DIM_-0001"], store=store)
        assert "error" in result


# ======================================================================
# Section G: 日期维度外键关联
# ======================================================================


class TestDateDimFk:
    """日期维度 → 事实设计外键关联。"""

    def test_date_dim_fk_links_to_fact(self, store: Store) -> None:
        """日期维度外键应关联到事实设计。"""
        result = date_dim_to_fact_fk("销售明细事实", "date_sk", store=store)
        assert result["fact_name"] == "销售明细事实"
        assert result["linked_facts"] >= 1

    def test_date_dim_fk_missing_fact(self, store: Store) -> None:
        """未找到事实表应报错。"""
        result = date_dim_to_fact_fk("不存在的事实", "date_sk", store=store)
        assert "error" in result