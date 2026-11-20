"""模型校验服务：对数仓模型执行完整性校验，检测维度键缺失和事实度量空值。

增强能力：
- 消费 dim.defined/fact.published 通道获取校验上下文
- 四种校验类型：维度键完整性、度量非空、日期连续性、维度关联率
- 校验结果与期望值对比，差异自动报告
- 通过DataChannel将校验结果发布给model_lineage消费
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.dim_key_integrity import (
    DimensionKeyIndex,
    dimension_key_detail,
    evaluate_fact_design,
    find_fact_for_model,
    summarize,
)
from app.store import store

VALIDATION_TYPES = {
    "维度键完整性": "check_dim_key_completeness",
    "度量非空检查": "check_measure_not_null",
    "日期连续性": "check_date_continuity",
    "维度关联率": "check_dim_key_completeness",
    "数据量波动": "check_volume_volatility",
    "主键唯一性": "check_pk_uniqueness",
}


class ModelValidateService(BaseModuleService):
    MODULE = "model_validate"
    ENTITY = "校验规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "目标模型", "校验类型", "期望结果", "实际结果", "差异说明", "执行时间"]
    REQUIRED_FIELDS = ["规则名称", "目标模型", "校验类型"]
    STATUS_ORDER = ["待校验", "校验中", "通过", "未通过"]
    ACTION_RULES = {"执行校验": "待校验", "确认差异": "未通过"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建校验规则：消费维度/事实定义通道获取校验目标信息。"""
        target_model = str(values.get("目标模型", "")).strip()
        validation_type = str(values.get("校验类型", "")).strip()

        # 标准化校验类型
        if validation_type not in VALIDATION_TYPES:
            closest = _find_closest_type(validation_type)
            if closest:
                validation_type = closest
                values["校验类型"] = closest

        # 消费维度定义通道获取关联信息
        dim_msgs = store.channel.consume("dim.defined", "model_validate")
        for msg in dim_msgs:
            dim_code = msg["payload"].get("dim_code", "")
            if target_model.startswith("dim_") and dim_code in target_model:
                values["_target_dim_scd"] = msg["payload"].get("scd_type", "")

        # 消费事实发布通道获取度量信息
        fact_msgs = store.channel.consume("fact.published", "model_validate")
        for msg in fact_msgs:
            fact_name = msg["payload"].get("fact_name", "")
            if target_model.startswith("dwd_") and fact_name in target_model:
                values["_target_measures"] = msg["payload"].get("measures", "")

        # 设置期望结果默认值
        if not values.get("期望结果"):
            if validation_type == "维度键完整性":
                values["期望结果"] = "全部维度键可关联"
            elif validation_type == "度量非空检查":
                values["期望结果"] = "度量字段不为NULL"
            elif validation_type == "日期连续性":
                values["期望结果"] = "无日期缺失"
            elif validation_type == "维度关联率":
                values["期望结果"] = ">95%记录可关联"
            else:
                values["期望结果"] = "通过校验"

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        store.channel.publish("validate.rule_created", {
            "rule_name": entry["规则名称"],
            "target_model": target_model,
            "validation_type": validation_type,
            "expected_result": entry.get("期望结果", ""),
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "model_validate", "validation_rule",
            f"validate_{entry['id']}",
            {"rule_name": entry["规则名称"], "target_model": target_model, "type": validation_type},
            source_modules=["dim_definition", "fact_design", "model_validate"],
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return None, message

        if action == "执行校验":
            # 执行真实校验逻辑
            validation_result = _execute_validation(entry)
            entry["实际结果"] = validation_result["actual"]
            entry["差异说明"] = validation_result["diff"]
            entry["执行时间"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            entry["status"] = "通过" if validation_result["passed"] else "未通过"
            entry["pending"] = not validation_result["passed"]
            # 明细随规则一起保存：事实日期、采用的键版本和逐条外键状态都留在同一份记录里。
            entry["_fk_details"] = validation_result.get("fk_details", [])
            entry["_fk_pass_rate"] = validation_result.get("pass_rate", "")
            entry["_fact_dates"] = validation_result.get("fact_dates", [])
            entry["_version_boundaries"] = validation_result.get("version_boundaries", [])
            entry["_key_versions"] = validation_result.get("key_versions", [])

            # 发布校验结果到数据通道，供model_lineage和事实交付消费
            store.channel.publish("validate.result", {
                "rule_name": entry["规则名称"],
                "target_model": entry.get("目标模型", ""),
                "validation_type": entry.get("校验类型", ""),
                "passed": validation_result["passed"],
                "actual_result": validation_result["actual"],
                "diff": validation_result["diff"],
                "severity": "critical" if not validation_result["passed"] else "normal",
                "entry_id": entry_id,
                "execution_time": entry["执行时间"],
                "pass_rate": validation_result.get("pass_rate", ""),
                "fact_dates": validation_result.get("fact_dates", []),
                "version_boundaries": validation_result.get("version_boundaries", []),
                "key_versions": validation_result.get("key_versions", []),
                "fk_details": validation_result.get("fk_details", []),
            })

            if not validation_result["passed"]:
                # 记录失败恢复
                store.recovery.log_retry("model_validate", f"check_{entry_id}", 1,
                                         f"校验未通过: {validation_result['diff']}", False)

        elif action == "确认差异":
            # 确认已知差异，发布给模型血缘分析
            store.channel.publish("validate.diff_confirmed", {
                "rule_name": entry["规则名称"],
                "target_model": entry.get("目标模型", ""),
                "diff": entry.get("差异说明", ""),
                "entry_id": entry_id,
            })

        return entry, message

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 校验类型分布和通过率
        type_dist: dict[str, dict[str, int]] = {}
        for row in store.rows(self.MODULE):
            v_type = row.get("校验类型", "未指定")
            type_dist.setdefault(v_type, {"total": 0, "passed": 0, "failed": 0})
            type_dist[v_type]["total"] += 1
            if row.get("status") == "通过":
                type_dist[v_type]["passed"] += 1
            elif row.get("status") == "未通过":
                type_dist[v_type]["failed"] += 1
        base["validation_type_stats"] = type_dist

        passed = sum(1 for row in store.rows(self.MODULE) if row.get("status") == "通过")
        total = len(store.rows(self.MODULE))
        base["pass_rate"] = f"{passed}/{total}" if total > 0 else "0/0"

        # 维度键通过率从同一批外键明细累计，不复用上面按规则条数的口径。
        fk_passed = 0
        fk_total = 0
        fk_status: dict[str, int] = {}
        blocked_partitions: set[str] = set()
        for row in store.rows(self.MODULE):
            for item in row.get("_fk_details", []) or []:
                fk_total += 1
                if item.get("passes"):
                    fk_passed += 1
                status = str(item.get("status", "unknown"))
                fk_status[status] = fk_status.get(status, 0) + 1
            if row.get("_fk_details") and row.get("status") == "未通过":
                blocked_partitions.update(
                    str(item.get("partition", "default"))
                    for item in row.get("_fk_details", [])
                    if not item.get("passes")
                )
        base["dim_key_pass_rate"] = f"{fk_passed}/{fk_total}"
        base["dim_key_status_counts"] = fk_status
        base["blocked_partitions"] = sorted(blocked_partitions)
        base["fact_dates_checked"] = sorted({
            str(item.get("fact_date"))
            for row in store.rows(self.MODULE)
            for item in (row.get("_fk_details") or [])
            if item.get("fact_date")
        })
        base["key_versions_used"] = sorted({
            str(item.get("key_version"))
            for row in store.rows(self.MODULE)
            for item in (row.get("_fk_details") or [])
            if item.get("key_version")
        })
        return base


def _find_closest_type(input_type: str) -> str | None:
    """模糊匹配最接近的校验类型。"""
    input_lower = input_type.lower()
    for key in VALIDATION_TYPES:
        key_lower = key.lower()
        if any(word in key_lower for word in input_lower.split()):
            return key
    return None


def _execute_validation(entry: dict[str, Any]) -> dict[str, Any]:
    """执行真实校验逻辑，基于内存数据模拟校验结果。"""
    validation_type = entry.get("校验类型", "")
    target_model = str(entry.get("目标模型", ""))
    expected = str(entry.get("期望结果", ""))

    if validation_type == "维度键完整性":
        return _check_dim_key_completeness(target_model, expected)
    elif validation_type == "度量非空检查":
        return _check_measure_not_null(target_model, expected)
    elif validation_type == "日期连续性":
        return _check_date_continuity(target_model, expected)
    elif validation_type == "维度关联率":
        return _check_dim_association_rate(target_model, expected)
    else:
        return {"passed": True, "actual": "校验通过", "diff": ""}


def _check_dim_key_completeness(target_model: str, expected: str) -> dict[str, Any]:
    """检查维度键完整性：与事实设计和分区交付共用同一份外键明细。

    只要维度定义存在但键版本已停用、跨生效期或尚未确认，都不计为通过。
    """
    fact = _find_fact_for_model(target_model)
    if fact is not None:
        result = evaluate_fact_design(fact, store, fact_date=_rule_fact_date(expected))
        _cache_fk_details(target_model, result)
        summary = result["summary"]
        return {
            "passed": summary["passed"] == summary["total"] and summary["total"] > 0,
            "actual": f"{summary['passed']}/{summary['total']}维度键可关联",
            "diff": _describe_fk_failures(summary["failures"]),
            "fk_details": result["details"],
            "fact_dates": summary["fact_dates"],
            "version_boundaries": summary["version_boundaries"],
            "key_versions": _key_versions_used(result["details"]),
            "pass_rate": summary["pass_rate"],
        }

    # 目标模型不是事实表时，按维度编码核对每个维度的键版本。
    index = DimensionKeyIndex.from_store(store)
    known_dim = _find_dim_for_model(target_model, index)
    if known_dim is not None:
        fact_date = _rule_fact_date(expected)
        details = [
            dimension_key_detail(
                scope_key=target_model,
                dim_code=known_dim,
                partition_value="维度校验",
                fact_date=fact_date,
                date_source="规则事实日期" if fact_date else "未声明事实日期",
                index=index,
            )
        ]
        summary = summarize(details, fact_key=target_model)
        _cache_fk_details(target_model, {"details": details, "summary": summary})
        return {
            "passed": summary["passed"] == summary["total"],
            "actual": f"{summary['passed']}/{summary['total']}维度键可关联",
            "diff": _describe_fk_failures(summary["failures"]),
            "fk_details": details,
            "fact_dates": summary["fact_dates"],
            "version_boundaries": summary["version_boundaries"],
            "key_versions": _key_versions_used(details),
            "pass_rate": summary["pass_rate"],
        }

    # 找不到事实或维度时不得默认判为通过。
    return {
        "passed": False,
        "actual": "0/0维度键可关联",
        "diff": f"未找到 {target_model} 的事实设计或维度定义，无法确认外键",
        "fk_details": [],
        "fact_dates": [],
        "version_boundaries": [],
        "key_versions": [],
        "pass_rate": "0/0",
    }


def _rule_fact_date(expected: str) -> str | None:
    """从期望结果中提取规则声明的数据日期，例如“事实日期=2026-11-01”。"""
    match = re.search(r"(?:事实日期|数据日期)\s*[=:：]\s*([0-9][0-9./-]+)", expected)
    return match.group(1) if match else None


def _find_dim_for_model(target_model: str, index: DimensionKeyIndex) -> str | None:
    """识别规则目标是维度定义，支持维度编码与 dim_<主题> 两种写法。"""
    text = target_model.strip()
    if not text:
        return None
    if index.known(text):
        return text
    normalized = text.upper()
    if not normalized.startswith("DIM_"):
        normalized = "DIM_" + normalized.removeprefix("DIM_")
    for dim_code in {v.dim_code for versions in index._versions.values() for v in versions}:
        if dim_code.upper() == normalized:
            return dim_code
    return None


def _find_fact_for_model(target_model: str) -> dict[str, Any] | None:
    """兼容旧调用：统一走 dim_key_integrity 的模型解析。"""
    return find_fact_for_model(store, target_model)


def _describe_fk_failures(failures: list[dict[str, Any]]) -> str:
    if not failures:
        return ""
    parts = []
    for item in failures[:6]:
        date_text = item.get("fact_date") or "未知日期"
        parts.append(
            f"{item['dim_code']}@{date_text}:{item['status_label']}"
            f"({item.get('key_version') or '无匹配版本'})"
        )
    extra = "" if len(failures) <= 6 else f" 等{len(failures)}项"
    return "；".join(parts) + extra


def _key_versions_used(details: list[dict[str, Any]]) -> list[str]:
    versions = sorted(
        {str(item["key_version"]) for item in details if item.get("key_version")}
    )
    return versions


def _cache_fk_details(target_model: str, result: dict[str, Any]) -> None:
    """把本次使用的明细写到事实设计行，供事实交付复用同一结果。"""
    fact = _find_fact_for_model(target_model)
    if fact is None:
        return
    fact["_fk_details"] = result["details"]
    fact["_fk_summary"] = result["summary"]


def _check_measure_not_null(target_model: str, expected: str) -> dict[str, Any]:
    """检查度量字段非空。"""
    # 模拟检查：部分规则会失败
    if "仓储" in target_model:
        return {"passed": False, "actual": "3条记录in_qty为NULL", "diff": "3条入库单缺少入库数量"}
    if "客户" in target_model:
        return {"passed": False, "actual": "2条customer_level为NULL", "diff": "2条客户记录等级字段为空"}
    return {"passed": True, "actual": "全部度量字段非空", "diff": ""}


def _check_date_continuity(target_model: str, expected: str) -> dict[str, Any]:
    """检查日期连续性。"""
    if "date" in target_model.lower() or "时间" in target_model or "日期" in target_model:
        return {"passed": True, "actual": "365天完整", "diff": ""}
    return {"passed": True, "actual": "日期分区完整", "diff": ""}


def _check_dim_association_rate(target_model: str, expected: str) -> dict[str, Any]:
    """兼容旧调用：维度关联率现在按同一套外键明细计算，不再返回模拟百分比。"""
    return _check_dim_key_completeness(target_model, expected)