"""事实表设计服务：管理事实表的业务过程、粒度和维度关联定义。

增强能力：
- 事实表名称标准化校验（前缀fact_）
- 维度关联完整性检查：关联维度必须已定义，且键在事实日期下处于已确认的
  生效期内；定义存在不等于键可用
- 与 model_validate、deploy_pipeline 共用 dim_key_integrity 的外键明细
- 登记/发布事实时按外键校验结果安排分区补键：只交付整分区通过的数据，
  需要更正的分区保留源记录，重新校验会核对实际改变的外键
- 发布事实时触发度量配置和聚合层自动建议
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.dim_key_integrity import (
    DimensionKeyIndex,
    collect_stored_fact_records,
    evaluate_fact_design,
    fact_dimension_fields,
    parse_fact_date,
    split_dim_codes,
    synthesize_design_records,
)
from app.services.pipeline import fact_design_partitions
from app.store import store


class FactDesignService(BaseModuleService):
    MODULE = "fact_design"
    ENTITY = "事实表"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["事实名称", "业务过程", "粒度声明", "关联维度", "度量字段", "分区字段", "更新模式"]
    REQUIRED_FIELDS = ["事实名称", "业务过程", "粒度声明"]
    STATUS_ORDER = ["草稿", "已设计", "已发布", "已冻结"]
    ACTION_RULES = {"提交设计": "草稿", "发布事实": "已设计", "冻结事实": "已发布"}
    NEGATIVE_ACTIONS = []

    UPDATE_MODES = {"每日追加", "每小时追加", "T+1批量", "实时追加", "每日全量"}

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建事实表定义：校验关联维度的存在性，标准化更新模式。"""
        # 标准化事实名称
        fact_name = str(values.get("事实名称", "")).strip()
        values["事实名称"] = fact_name

        # 标准化更新模式
        update_mode = str(values.get("更新模式", "每日追加")).strip()
        if update_mode not in self.UPDATE_MODES:
            update_mode = "每日追加"
            values["更新模式"] = update_mode

        # 校验关联维度存在性
        related_dims = str(values.get("关联维度", "")).strip()
        validated_dims = _validate_dimensions(related_dims)
        if not validated_dims:
            values["关联维度"] = related_dims  # 保留原始输入

        # 消费已发布维度消息
        dim_msgs = store.channel.consume("dim.published", "fact_design")
        for msg in dim_msgs:
            dim_code = msg["payload"].get("dim_code", "")
            if dim_code in related_dims:
                # 确认维度可用
                values[f"_dim_{dim_code}_confirmed"] = True

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        # 登记时带上了分区记录：先按统一规则校验并只交付通过的分区。
        partition_records = values.get("分区记录")
        if isinstance(partition_records, list) and partition_records:
            delivery = fact_design_partitions(
                entry, [dict(item) for item in partition_records], store,
                auto_refill=bool(values.get("自动补键", True)),
            )
            entry["_fk_details"] = delivery["fk_details"]
            entry["_fk_summary"] = delivery["summary"]
            entry["_delivery"] = {
                "pass_rate": delivery["pass_rate"],
                "delivered_partitions": delivery["delivered_partitions"],
                "blocked_partitions": delivery["blocked_partitions"],
                "corrections": delivery["corrections"],
            }
            entry["_partition_records"] = delivery["records"]

        # 发布事实表定义到数据通道
        store.channel.publish("fact.defined", {
            "fact_name": entry["事实名称"],
            "business_process": entry["业务过程"],
            "granularity": entry["粒度声明"],
            "related_dims": related_dims,
            "measures": entry.get("度量字段", ""),
            "partition_field": entry.get("分区字段", ""),
            "update_mode": entry["更新模式"],
            "entry_id": entry["id"],
        })

        # 注册事实产物
        store.artifacts.register(
            "fact_design", "fact_table",
            f"fact_{entry['id']}",
            {"fact_name": entry["事实名称"], "business_process": entry["业务过程"]},
            source_modules=["dim_definition", "fact_design"],
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return None, message

        if action == "发布事实":
            # 用统一的外键明细判断能否发布：定义存在但键已停用/跨生效期不算通过。
            validation = _validate_fact_dim_completeness(entry)
            if not validation["valid"]:
                return None, validation["message"]

            # 事实交付与校验共用同一份明细，按分区安排补键和拦截。
            delivery = _deliver_fact_partitions(entry)
            entry["_fk_details"] = delivery["fk_details"]
            entry["_fk_summary"] = delivery["summary"]
            entry["_delivery"] = {
                "pass_rate": delivery["pass_rate"],
                "delivered_partitions": delivery["delivered_partitions"],
                "blocked_partitions": delivery["blocked_partitions"],
                "corrections": delivery["corrections"],
                "fact_dates": delivery["fact_dates"],
                "key_versions": sorted({
                    str(item["key_version"]) for item in delivery["fk_details"]
                    if item.get("key_version")
                }),
            }
            if delivery["blocked_partitions"]:
                return entry, (
                    f"事实表已发布，但分区 "
                    f"{'、'.join(str(item['partition']) for item in delivery['blocked_partitions'])} "
                    f"存在维度外键问题，已保留源记录等待更正；"
                    f"通过 {delivery['pass_rate']}"
                )

            store.channel.publish("fact.published", {
                "fact_name": entry["事实名称"],
                "business_process": entry["业务过程"],
                "granularity": entry["粒度声明"],
                "related_dims": entry.get("关联维度", ""),
                "measures": entry.get("度量字段", ""),
                "update_mode": entry.get("更新模式"),
                "entry_id": entry["id"],
                "fk_pass_rate": delivery["pass_rate"],
                "fact_dates": delivery["fact_dates"],
                "key_versions": sorted({
                    str(item["key_version"]) for item in delivery["fk_details"]
                    if item.get("key_version")
                }),
                "fk_details": delivery["fk_details"],
            })

            # 自动建议度量配置
            _suggest_measures(entry)
            # 自动建议聚合汇总
            _suggest_aggregation(entry)

        elif action == "冻结事实":
            store.channel.publish("fact.frozen", {
                "fact_name": entry["事实名称"],
                "entry_id": entry["id"],
            })

        return entry, message

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        dim_consumed = len(store.channel.consume("dim.published", "fact_design"))
        base["consumed_published_dims"] = dim_consumed

        # 维度关联矩阵
        dim_matrix: dict[str, list[str]] = {}
        for row in store.rows(self.MODULE):
            fact_name = row.get("事实名称", "")
            dims = str(row.get("关联维度", "")).split(".")
            dim_matrix[fact_name] = [d for d in dims if d]
        base["dimension_association_matrix"] = dim_matrix

        # 外键交付概况：通过率、被拦截分区和采用的键版本全部来自同一份明细。
        base["fk_pass_rate"] = {}
        base["blocked_partitions"] = {}
        base["key_versions_used"] = []
        base["version_boundaries"] = []
        versions: set[str] = set()
        boundaries: set[str] = set()
        for row in store.rows(self.MODULE):
            summary = row.get("_fk_summary")
            if not summary:
                continue
            fact_name = row.get("事实名称", "")
            base["fk_pass_rate"][fact_name] = summary["pass_rate"]
            blocked = [
                str(item["partition"])
                for item in (row.get("_delivery") or {}).get("blocked_partitions", [])
            ]
            if blocked:
                base["blocked_partitions"][fact_name] = blocked
            versions.update(
                str(item["key_version"])
                for item in row.get("_fk_details", [])
                if item.get("key_version")
            )
            boundaries.update(summary.get("version_boundaries", []))
        base["key_versions_used"] = sorted(versions)
        base["version_boundaries"] = sorted(boundaries)
        return base


def _validate_dimensions(dim_str: str) -> list[str]:
    """校验关联维度字符串，过滤有效维度编码。"""
    if not dim_str.strip():
        return []
    dim_codes = [d.strip() for d in dim_str.split(".") if d.strip()]
    index = DimensionKeyIndex.from_store(store)
    return [d for d in dim_codes if index.known(d)]


def _validate_fact_dim_completeness(entry: dict[str, Any]) -> dict[str, Any]:
    """校验事实表关联维度完整性：定义存在且键版本可用。"""
    related_dims = str(entry.get("关联维度", "")).strip()
    if not related_dims:
        return {"valid": False, "message": "事实表至少需要关联一个维度"}

    dim_codes = split_dim_codes(related_dims)
    index = DimensionKeyIndex.from_store(store)
    missing = [d for d in dim_codes if not index.known(d)]
    if missing:
        return {"valid": False, "message": f"维度 {'、'.join(missing)} 未定义，请先创建维度定义"}

    # 只拦“没有任何分区记录可交付”的情况；具体的键版本问题交给交付环节
    # 按分区处理，避免因为某个历史分区有问题就整表无法发布。
    records = collect_stored_fact_records(store, str(entry.get("事实名称", "")))
    if not records:
        records = [dict(item) for item in entry.get("_partition_records") or []]
    candidate = _delivery_records(entry) if not records else records
    evaluation = evaluate_fact_design(entry, store, records=candidate)
    summary = evaluation["summary"]
    if summary["passed"] == 0 and summary["total"] > 0:
        reasons = "、".join(
            f"{item['dim_code']}({item['status_label']})" for item in summary["failures"][:6]
        )
        return {"valid": False, "message": f"维度键全部不可用：{reasons}，请先确认维度键版本"}
    return {"valid": True, "message": ""}


def _delivery_records(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """取回待校验的同一批分区记录。

    已落库的分区记录优先；没有的话用一次性登记时提交的批次；
    两者都没有时，设计自身的维度关联就是一条样本记录。
    """
    submitted = [dict(item) for item in entry.get("_partition_records") or []]
    # 一次性登记时提交的整批记录优先：已交付的分区已经落库，被拦截的
    # 分区只留在批次里，必须一起取回才能保证重复校验覆盖同一批记录。
    records = submitted or collect_stored_fact_records(
        store, str(entry.get("事实名称", ""))
    )
    if not records:
        # 首次发布没有独立分区记录时，用设计声明的真实键值生成样本记录。
        index = DimensionKeyIndex.from_store(store)
        records = synthesize_design_records(
            entry,
            fact_dimension_fields(entry, index),
            parse_fact_date(entry.get("_fact_date")),
            index,
        )
    return records


def _deliver_fact_partitions(entry: dict[str, Any]) -> dict[str, Any]:
    """事实设计发布时按外键校验结果安排分区补键和交付。"""
    return fact_design_partitions(
        entry,
        _delivery_records(entry),
        store,
        auto_refill=True,
        previous_details=entry.get("_fk_details") or [],
    )


def _suggest_measures(fact: dict[str, Any]) -> None:
    """基于事实表定义自动建议度量配置。"""
    measures_str = str(fact.get("度量字段", "")).strip()
    if not measures_str:
        return

    measure_names = [m.strip() for m in measures_str.split(".") if m.strip()]
    for m_name in measure_names:
        measure_type = "DECIMAL(18,2)" if "_amt" in m_name else "BIGINT"
        store.channel.publish("measure.suggested", {
            "measure_name": m_name,
            "fact_name": fact["事实名称"],
            "measure_type": measure_type,
            "agg_method": "SUM" if ("_amt" in m_name or "_qty" in m_name) else "COUNT",
            "source_fact_id": fact.get("id"),
            "is_additive": "_cnt" not in m_name and "_rate" not in m_name,
        })


def _suggest_aggregation(fact: dict[str, Any]) -> None:
    """基于事实表定义自动建议汇总表配置。"""
    fact_name = fact.get("事实名称", "")
    store.channel.publish("agg.suggested", {
        "agg_name": f"每日{fact_name}汇总",
        "source_fact": fact_name,
        "agg_dims": fact.get("关联维度", ""),
        "agg_measures": fact.get("度量字段", ""),
        "agg_granularity": "每日",
        "source_fact_id": fact.get("id"),
    })