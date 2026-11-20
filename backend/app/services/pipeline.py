"""数据流管线服务：提供维度建模核心业务操作。
贯通链路：
  dim_definition → dim_attr → scd_handler → surrogate_key → slow_change_log
  fact_design → measure_config → star_query → agg_summary
  etl_mapping → deploy_pipeline → model_validate
  hierarchy_build → dim_bridge → cube_builder
  conform_dim → dim_definition 关联约束
  snapshot_plan → data_vault → model_lineage
  date_dim_gen → fact_design 外键关联
"""
from __future__ import annotations

import time
import uuid
from typing import Any, Optional

from app.services.dim_key_integrity import (  # 分区校验与事实设计共用同一实现
    OUTSIDE_EFFECTIVE_PERIOD,
    DimensionKeyIndex,
    diff_foreign_keys,
    evaluate_records,
    fact_dimension_fields,
    parse_fact_date,
    refill_missing_keys,
)

# ---------------------------------------------------------------------------
# 1. SCD 类型 2 处理
# ---------------------------------------------------------------------------


def scd_type2_handle(
    dimension_key: str,
    new_attrs: dict[str, Any],
    store: Any,
    *,
    change_date: str | None = None,
) -> dict[str, Any]:
    """对某个维度执行 SCD Type 2 变更：旧记录失效，新记录生效。

    1. 查找该维度当前生效记录（current_marker=1）
    2. 标记旧记录失效（current_marker=0, end_date=now）
    3. 插入新记录（current_marker=1, start_date=now）

    change_date 用于回放历史换键：旧版本的失效日与新版本的生效日都取该日期，
    避免非今天的变更把 9999-12-31 这类哨兵时间写进区间比较。
    """
    now_str = change_date or time.strftime("%Y-%m-%d %H:%M:%S")
    result: dict[str, Any] = {"dimension_key": dimension_key, "action": "scd2_change"}

    # 在内存 store 中查找属于该维度的记录
    dim_table = store.rows("dim_definition")
    old_records = [
        r for r in dim_table
        if str(r.get("维度编码", "")) == dimension_key and str(r.get("current_marker", "1")) == "1"
    ]

    expired_count = 0
    for old in old_records:
        old["current_marker"] = "0"
        old["失效时间"] = now_str
        old["status"] = "已停用"
        expired_count += 1

    # 插入新记录：沿用原键值集合（换键时由 new_attrs 覆盖），保证历史事实
    # 仍能在旧版本里、新事实能在新版本里解析到该维度。
    new_id = max((int(r.get("id", 0)) for r in dim_table), default=0) + 1
    new_row = {
        "id": new_id,
        "维度编码": dimension_key,
        "status": "已发布",
        "pending": True,
        "abnormal": False,
    }
    inherited_keys = {
        str(r.get("键值")) for r in old_records if str(r.get("键值", "")).strip()
    }
    if inherited_keys:
        new_row["键值"] = ".".join(sorted(inherited_keys))
    # 合并新属性
    new_row.update({k: str(v) for k, v in new_attrs.items()})
    new_row["current_marker"] = "1"
    new_row["生效时间"] = now_str
    new_row["失效时间"] = "9999-12-31 23:59:59"
    dim_table.append(new_row)

    # 记入变更日志
    log_table = store.rows("slow_change_log")
    log_id = max((int(r.get("id", 0)) for r in log_table), default=0) + 1
    changed_fields = ", ".join(new_attrs.keys())
    log_table.append({
        "id": log_id,
        "变更ID": f"SCD-{uuid.uuid4().hex[:8].upper()}",
        "目标维度": dimension_key,
        "变更字段": changed_fields,
        "旧值": str([r.get(f, "") for f in new_attrs for r in old_records if r.get(f)]) if old_records else "N/A",
        "新值": str(list(new_attrs.values())),
        "变更时间": now_str,
        "变更原因": f"SCD Type 2 变更: {changed_fields}",
        "status": "已记录",
        "pending": True,
        "abnormal": False,
    })

    result["expired_count"] = expired_count
    result["new_id"] = new_id
    result["log_id"] = log_id
    return result


# ---------------------------------------------------------------------------
# 2. 代理键分配与冲突处理
# ---------------------------------------------------------------------------

class SurrogateKeyAllocator:
    """代理键生成器：支持顺序递增与键段预分配，检测冲突并重新分配。"""

    def __init__(self, store: Any) -> None:
        self._store = store
        self._counters: dict[str, int] = {}
        self._assigned: dict[str, set[int]] = {}

    def allocate(self, dimension_key: str, strategy: str = "sequence") -> int:
        """为指定维度分配一个代理键。

        顺序模式：从 counter 递增
        键段模式：从 seed 数据中读取初始值和步长
        冲突检测：已分配的键不回退重用
        """
        table = self._store.rows("surrogate_key")
        configs = [r for r in table if str(r.get("目标维度", "")) == dimension_key]

        if strategy == "sequence":
            if dimension_key not in self._counters:
                start = 1
                for cfg in configs:
                    try:
                        start = int(cfg.get("初始值", "1"))
                    except (ValueError, TypeError):
                        start = 1
                self._counters[dimension_key] = start
                self._assigned.setdefault(dimension_key, set())

            key = self._counters[dimension_key]
            self._counters[dimension_key] += 1
            self._assigned[dimension_key].add(key)
            return key

        elif strategy == "segment":
            step = 1000
            for cfg in configs:
                try:
                    step = int(cfg.get("步长", "1000"))
                except (ValueError, TypeError):
                    pass
            if dimension_key not in self._counters:
                self._counters[dimension_key] = 10000
            key = self._counters[dimension_key]
            self._counters[dimension_key] += step
            self._assigned.setdefault(dimension_key, set()).add(key)
            return key

        return 0

    def handle_conflict(self, dimension_key: str, conflicting_key: int) -> int:
        """发生代理键冲突时的处理：将冲突键标记后重新分配下一个键。"""
        if dimension_key not in self._counters:
            self._counters[dimension_key] = 100000
        new_key = self._counters[dimension_key]
        self._counters[dimension_key] += 1
        return new_key

    def get_assigned_keys(self, dimension_key: str) -> list[int]:
        return sorted(self._assigned.get(dimension_key, set()))


# ---------------------------------------------------------------------------
# 3. 事实表分区写入与维度外键完整性校验
# ---------------------------------------------------------------------------

def fact_partition_write(
    fact_key: str,
    records: list[dict[str, Any]],
    partition_field: str,
    dim_fk_fields: dict[str, str],
    store: Any,
    *,
    auto_refill: bool = False,
    previous_details: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """向事实表写入记录，同时校验维度外键完整性。

    校验明细由 dim_key_integrity 统一产出：按每条记录的事实日期核对键版本，
    已停用、跨生效期、未确认和完全不存在的键都不会被计为通过。交付只接纳
    整个分区都通过的记录；失败分区的源记录原样保留，等待更正后重新校验。

    Args:
        fact_key: 事实表标识
        records: 待写入的事实记录列表
        partition_field: 分区字段名
        dim_fk_fields: 维度外键映射 {field_name: dim_key}
        store: 数据仓库实例
        auto_refill: 是否为跨生效期的历史事实补当前键版本
        previous_details: 上一次校验的明细，用于核对实际改变的外键

    Returns:
        {wrote, violations, partitions, fk_details, pass_rate, ...}
    """
    index = DimensionKeyIndex.from_store(store)
    details, summary = evaluate_records(
        fact_key=fact_key,
        records=records,
        dim_fk_fields=dim_fk_fields,
        partition_field=partition_field,
        index=index,
    )

    refills: list[dict[str, Any]] = []
    if auto_refill and summary["failures"]:
        for failure in summary["failures"]:
            if failure["status"] != OUTSIDE_EFFECTIVE_PERIOD:
                continue
            # 只有“事实日期落在所有版本区间之外”的历史事实需要补键；
            # 已停用、未确认和不存在都属于需要人工更正的键，补版本只会掩盖问题。
            refill = refill_missing_keys(
                store,
                failure["dim_code"],
                parse_fact_date(failure.get("fact_date")),
                key_value=failure.get("value", ""),
            )
            if refill["refilled"]:
                refills.append(refill)
        if refills:
            # 补键后按同一批记录、同一批分区整体重算：跨版本边界的分区
            # 不会因为上一次已经通过而被跳过。
            index = DimensionKeyIndex.from_store(store)
            details, summary = evaluate_records(
                fact_key=fact_key,
                records=records,
                dim_fk_fields=dim_fk_fields,
                partition_field=partition_field,
                index=index,
            )

    result: dict[str, Any] = {
        "wrote": 0,
        "violations": [],
        "partitions": {},
        "records": [dict(record) for record in records],
        "fk_details": details,
        "pass_rate": summary["pass_rate"],
        "fact_dates": summary["fact_dates"],
        "version_boundaries": summary["version_boundaries"],
        "status_counts": summary["status_counts"],
        "summary": summary,
        "refills": refills,
        "delivered_partitions": [],
        "blocked_partitions": [],
        "corrections": [],
        "changed_foreign_keys": [],
    }

    delivered_indexes: set[int] = set()
    record_partitions = {
        int(item["record_index"]): item["partition"]
        for item in details
        if "record_index" in item
    }
    for partition, bucket in summary["partitions"].items():
        if bucket["failed"] == 0:
            result["delivered_partitions"].append(partition)
            for item in details:
                if item["partition"] == partition:
                    delivered_indexes.add(int(item["record_index"]))
        else:
            result["blocked_partitions"].append({
                "partition": partition,
                "failed": bucket["failed"],
                "statuses": bucket["statuses"],
                "fact_dates": bucket["fact_dates"],
            })
            for offset, record in enumerate(records):
                if record_partitions.get(offset) != partition:
                    continue
                # 整分区拦截：源记录原样保留，更正后重新校验同一批记录。
                result["corrections"].append({
                    "record_index": offset,
                    "partition": partition,
                    "fact_date": bucket["fact_dates"],
                    "record": dict(record),
                })

    for item in details:
        if not item["passes"]:
            result["violations"].append({
                "field": item["field"],
                "value": item["value"],
                "reason": (
                    f"维度 {item['dim_code']} 在事实日期 {item['fact_date'] or '未知'} "
                    f"{item['status_label']} {item['value'] or '(空)'}"
                ),
                "status": item["status"],
                "fact_date": item["fact_date"],
                "partition": item["partition"],
                "key_version": item["key_version"],
            })

    fact_table = store.rows("fact_design")
    max_id = max((int(r.get("id", 0)) for r in fact_table), default=0)
    for offset, record in enumerate(records):
        if offset not in delivered_indexes:
            continue
        max_id += 1
        partition_value = record_partitions.get(offset, "default")
        result["partitions"].setdefault(partition_value, 0)
        result["partitions"][partition_value] += 1

        row = {
            "id": max_id,
            "事实名称": fact_key,
            "status": "已设计",
            "pending": True,
            "abnormal": False,
            "_partition_record": True,
        }
        row.update({k: str(v) for k, v in record.items()})
        fact_table.append(row)
        result["wrote"] += 1

    result["changed_foreign_keys"] = diff_foreign_keys(previous_details or [], details)
    return result


def partition_field_name(partition_field: str | None) -> str:
    """从“dt DATE”这类声明里取出真正的分区字段名。"""
    if not partition_field:
        return ""
    return str(partition_field).strip().split()[0].split("=")[-1]


def fact_design_partitions(
    fact: dict[str, Any],
    records: list[dict[str, Any]],
    store: Any,
    *,
    auto_refill: bool = True,
    previous_details: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """事实设计发布前按外键校验结果安排分区补键与交付。

    与 model_validate 的完整性校验共用 dim_key_integrity 的明细：
    可交付的分区继续走 fact_partition_write，需更正的分区保留源记录。
    """
    index = DimensionKeyIndex.from_store(store)
    dim_fk_fields = fact_dimension_fields(fact, index)
    partition_field = partition_field_name(fact.get("分区字段", ""))
    return fact_partition_write(
        fact_key=fact.get("事实名称", ""),
        records=records,
        partition_field=partition_field,
        dim_fk_fields=dim_fk_fields,
        store=store,
        auto_refill=auto_refill,
        previous_details=previous_details,
    )



# ---------------------------------------------------------------------------
# 4. 汇总表构建与增量刷新
# ---------------------------------------------------------------------------

class AggregateBuilder:
    """汇总表构建器：基于事实表上卷维度、聚合度量。支持全量构建与增量刷新。"""

    def __init__(self, store: Any) -> None:
        self._store = store
        self._watermarks: dict[str, int] = {}

    def build(self, agg_name: str, source_fact: str, rollup_dims: list[str],
              agg_measures: list[tuple[str, str]], grain: str) -> dict[str, Any]:
        """全量构建汇总表。

        Args:
            agg_name: 汇总表名称
            source_fact: 源事实表名称
            rollup_dims: 上卷维度列表
            agg_measures: 聚合度量 [(measure_name, agg_func)]
            grain: 汇总粒度
        """
        fact_table = self._store.rows("fact_design")
        source_rows = [r for r in fact_table if str(r.get("事实名称", "")) == source_fact]

        result = self._compute_aggregation(source_rows, rollup_dims, agg_measures)

        # 写入汇总表
        agg_table = self._store.rows("agg_summary")
        max_id = max((int(r.get("id", 0)) for r in agg_table), default=0)
        for group_key, measures in result.items():
            max_id += 1
            row = {
                "id": max_id,
                "汇总名称": agg_name,
                "源事实表": source_fact,
                "汇总维度": ", ".join(rollup_dims),
                "汇总度量": str(measures),
                "汇总粒度": grain,
                "刷新周期": "T+1",
                "增量标记": "full",
                "status": "已构建",
                "pending": False,
                "abnormal": False,
            }
            agg_table.append(row)

        # 更新水位
        self._watermarks[agg_name] = max_id
        return {"built_groups": len(result), "total_rows": max_id, "mode": "full"}

    def refresh_incremental(self, agg_name: str, source_fact: str,
                            rollup_dims: list[str], agg_measures: list[tuple[str, str]],
                            since_id: int) -> dict[str, Any]:
        """增量刷新汇总表。

        只处理 id > since_id 的新增事实记录，追加汇总结果。
        """
        fact_table = self._store.rows("fact_design")
        new_rows = [
            r for r in fact_table
            if str(r.get("事实名称", "")) == source_fact
            and int(r.get("id", 0)) > since_id
        ]

        if not new_rows:
            return {"built_groups": 0, "new_rows": 0, "mode": "incremental"}

        result = self._compute_aggregation(new_rows, rollup_dims, agg_measures)

        agg_table = self._store.rows("agg_summary")
        max_id = max((int(r.get("id", 0)) for r in agg_table), default=0)
        for group_key, measures in result.items():
            max_id += 1
            row = {
                "id": max_id,
                "汇总名称": agg_name,
                "源事实表": source_fact,
                "汇总维度": ", ".join(rollup_dims),
                "汇总度量": str(measures),
                "增量标记": "incremental",
                "status": "已刷新",
                "pending": False,
                "abnormal": False,
            }
            agg_table.append(row)

        self._watermarks[agg_name] = max_id
        return {"built_groups": len(result), "new_rows": len(new_rows), "mode": "incremental"}

    def _compute_aggregation(
        self,
        rows: list[dict[str, Any]],
        rollup_dims: list[str],
        agg_measures: list[tuple[str, str]],
    ) -> dict[str, dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        for row in rows:
            key_parts = [str(row.get(d, "N/A")) for d in rollup_dims]
            key = "|".join(key_parts)
            if key not in groups:
                groups[key] = {d: row.get(d, "") for d in rollup_dims}
                for m_name, _ in agg_measures:
                    groups[key][f"_{m_name}_cnt"] = 0.0
                    groups[key][f"_{m_name}_sum"] = 0.0
                groups[key]["_row_count"] = 0

            groups[key]["_row_count"] += 1
            for m_name, agg_func in agg_measures:
                try:
                    raw_val = float(str(row.get(m_name, "0")).replace(",", ""))
                except (ValueError, TypeError):
                    raw_val = 0.0

                if agg_func in ("sum", "SUM"):
                    groups[key][f"_{m_name}_sum"] += raw_val
                if agg_func in ("count", "COUNT"):
                    groups[key][f"_{m_name}_cnt"] += 1
                if agg_func in ("avg", "AVG"):
                    groups[key][f"_{m_name}_cnt"] += 1
                    groups[key][f"_{m_name}_sum"] += raw_val

            # 计算 AVG
            for m_name, agg_func in agg_measures:
                if agg_func in ("avg", "AVG") and groups[key][f"_{m_name}_cnt"] > 0:
                    groups[key][f"{m_name}_avg"] = (
                        groups[key][f"_{m_name}_sum"] / groups[key][f"_{m_name}_cnt"]
                    )

        return groups


# ---------------------------------------------------------------------------
# 5. 模型血缘影响分析
# ---------------------------------------------------------------------------


def model_lineage_impact(
    changed_model: str,
    changed_fields: list[str],
    store: Any,
) -> dict[str, Any]:
    """分析模型变更对下游的影响范围。

    遍历 model_lineage 表，找到所有下游依赖，按影响级别分类。
    返回下游模型列表和受影响字段。
    """
    lineage_table = store.rows("model_lineage")
    affected: list[dict[str, Any]] = []

    for rel in lineage_table:
        upstream = str(rel.get("上游模型", ""))
        if upstream == changed_model or any(
            f in str(rel.get("依赖字段", "")) for f in changed_fields
        ):
            impact = {
                "relationship_id": rel.get("关系ID", rel.get("id", "")),
                "downstream": rel.get("下游模型", ""),
                "dependency": rel.get("依赖字段", ""),
                "type": rel.get("关联类型", "direct"),
                "level": rel.get("影响级别", "medium"),
                "current_status": rel.get("更新状态", rel.get("status", "")),
            }

            # 更新影响状态
            rel["status"] = "变更中"
            rel["更新状态"] = f"受 {changed_model} 变更影响"
            affected.append(impact)

    # 计算影响摘要
    severe = [a for a in affected if a.get("level") == "high"]
    moderate = [a for a in affected if a.get("level") == "medium"]

    return {
        "changed_model": changed_model,
        "changed_fields": changed_fields,
        "affected_count": len(affected),
        "severe_impact": len(severe),
        "moderate_impact": len(moderate),
        "downstream_models": list({a["downstream"] for a in affected}),
        "details": affected,
    }


# ---------------------------------------------------------------------------
# 6. 维度定义关联约束（一致性维度）
# ---------------------------------------------------------------------------


def conform_dim_constrain(
    conform_name: str,
    required_dim_keys: list[str],
    store: Any,
) -> dict[str, Any]:
    """将一致性维度标准与维度定义做关联约束校验。

    检查所有 dim_definition 记录是否符合一致性维度标准。
    """
    dim_table = store.rows("dim_definition")
    conform_table = store.rows("conform_dim")

    conform_entry = None
    for c in conform_table:
        if str(c.get("标准名称", "")) == conform_name:
            conform_entry = c
            break

    if not conform_entry:
        return {"error": f"未找到一致性维度 {conform_name}"}

    violations: list[dict[str, Any]] = []
    passed: list[str] = []

    for dim in dim_table:
        dim_key = str(dim.get("维度编码", ""))
        if dim_key in required_dim_keys:
            dim_name = str(dim.get("维度名称", ""))
            standard_key = str(conform_entry.get("标准键值", ""))
            if dim_name and standard_key and dim_name != standard_key:
                violations.append({
                    "dim_key": dim_key,
                    "dim_name": dim_name,
                    "expected": standard_key,
                    "reason": f"维度名称与一致性标准 {conform_name} 不一致",
                })
            else:
                passed.append(dim_key)

    return {
        "conform_name": conform_name,
        "total_checked": len(required_dim_keys),
        "passed": len(passed),
        "violations": len(violations),
        "violation_details": violations,
    }


# ---------------------------------------------------------------------------
# 7. 日期维度 → 事实设计外键关联
# ---------------------------------------------------------------------------


def date_dim_to_fact_fk(
    fact_name: str,
    date_fk_field: str,
    store: Any,
) -> dict[str, Any]:
    """将日期维度表的外键关联到事实表中。

    为指定事实表添加日期维度外键约束，记录在 date_dim_gen 模块中。
    """
    fact_table = store.rows("fact_design")
    date_table = store.rows("date_dim_gen")

    # 查找目标事实表
    target_facts = [r for r in fact_table if str(r.get("事实名称", "")) == fact_name]
    if not target_facts:
        return {"error": f"未找到事实表 {fact_name}"}

    # 可用的日期键
    date_keys = [str(r.get("日期键", "")) for r in date_table if r.get("日期键")]

    for fact in target_facts:
        existing_dims = str(fact.get("关联维度", ""))
        if date_fk_field not in existing_dims:
            fact["关联维度"] = f"{existing_dims},{date_fk_field}" if existing_dims else date_fk_field

    return {
        "fact_name": fact_name,
        "date_fk_field": date_fk_field,
        "linked_facts": len(target_facts),
        "available_date_keys": len(date_keys),
    }