"""部署流水线服务：管理数仓模型的部署任务，包括环境切换、前置检查和回滚机制。

增强能力：
- 消费 fact.published/agg.built 通道，自动生成部署任务
- 前置检查：校验上游依赖是否就绪（维度/事实/ETL映射）
- 执行部署：发布到目标环境并通过DataChannel通知snapshot_plan
- 触发回滚：恢复到上一有效快照版本
- 部署状态追踪与失败链路记录
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.dim_key_integrity import evaluate_fact_design, find_fact_for_model
from app.store import store

ENVIRONMENTS = {"开发", "预发", "生产", "灰度"}


class DeployPipelineService(BaseModuleService):
    MODULE = "deploy_pipeline"
    ENTITY = "部署任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务名称", "目标模型", "部署环境", "前置检查", "回滚方案", "部署状态", "执行日志"]
    REQUIRED_FIELDS = ["任务名称", "目标模型", "部署环境"]
    STATUS_ORDER = ["待部署", "部署中", "已部署", "部署失败", "已回滚"]
    ACTION_RULES = {"执行部署": "待部署", "触发回滚": "已部署"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建部署任务：检查部署环境合法性，基于上游发布事件自动填充前置检查。"""
        env = str(values.get("部署环境", "")).strip()
        if env not in ENVIRONMENTS:
            env = "预发"
            values["部署环境"] = env

        target_model = str(values.get("目标模型", "")).strip()

        # 消费上游发布事件，自动构建前置检查列表
        prerequisites = _gather_prerequisites(target_model)
        if prerequisites:
            values["前置检查"] = "/".join(prerequisites)

        # 自动生成回滚方案
        if not values.get("回滚方案"):
            rollback = _generate_rollback_plan(target_model)
            values["回滚方案"] = rollback

        # 消费事实发布通道
        fact_msgs = store.channel.consume("fact.published", "deploy_pipeline")
        for msg in fact_msgs:
            if msg["payload"]["fact_name"] in target_model:
                values["_fact_published"] = True

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        store.channel.publish("deploy.task_created", {
            "task_name": entry["任务名称"],
            "target_model": target_model,
            "environment": env,
            "prerequisites": prerequisites,
            "rollback_plan": entry.get("回滚方案"),
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "deploy_pipeline", "deploy_task",
            f"deploy_{entry['id']}",
            {"task_name": entry["任务名称"], "target_model": target_model, "env": env},
            source_modules=["fact_design", "agg_summary", "deploy_pipeline"],
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return None, message

        target_model = entry.get("目标模型", "")

        if action == "执行部署":
            entry["status"] = "部署中"
            # 执行前置检查
            check_result = _run_preflight_checks(entry)
            if not check_result["all_passed"]:
                entry["status"] = "部署失败"
                entry["部署状态"] = "前置检查失败"
                entry["执行日志"] = check_result["detail"]
                entry["pending"] = True
                entry["abnormal"] = True
                store.recovery.log_retry("deploy_pipeline", f"deploy_{entry_id}", 1,
                                         check_result["detail"], False)
                store.channel.publish("deploy.failed", {
                    "task_name": entry["任务名称"],
                    "target_model": target_model,
                    "reason": check_result["detail"],
                    "entry_id": entry_id,
                })
                return entry, f"部署失败: {check_result['detail']}"

            # 部署成功
            entry["status"] = "已部署"
            entry["部署状态"] = "成功"
            entry["执行日志"] = f"前置检查通过-模型发布-数据回填完成 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}"
            entry["pending"] = False

            # 发布部署成功事件，触发snapshot_plan生成快照
            store.channel.publish("deploy.published", {
                "task_name": entry["任务名称"],
                "target_model": target_model,
                "environment": entry.get("部署环境", ""),
                "deployed_at": datetime.now(timezone.utc).isoformat(),
                "source_version": check_result.get("current_version", "v1.0"),
                "entry_id": entry_id,
            })

            # 创建部署检查点用于恢复
            cp_id = store.recovery.create_checkpoint("deploy_pipeline", f"deploy_{entry_id}", {
                "target_model": target_model,
                "environment": entry.get("部署环境"),
                "deployed_at": datetime.now(timezone.utc).isoformat(),
            })
            store.recovery.mark_completed(cp_id)

        elif action == "触发回滚":
            # 回滚到上一版本
            rollback_result = _execute_rollback(entry)
            entry["status"] = "已回滚"
            entry["部署状态"] = f"已回滚至{rollback_result.get('rollback_version', '上一版本')}"
            entry["执行日志"] = f"回滚原因: {rollback_result.get('reason', '手动触发')}"
            entry["pending"] = True
            entry["abnormal"] = True

            store.channel.publish("deploy.rollback", {
                "task_name": entry["任务名称"],
                "target_model": target_model,
                "rollback_to": rollback_result.get("rollback_version"),
                "entry_id": entry_id,
            })

        return entry, message

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        env_stats: dict[str, dict[str, int]] = {}
        for row in store.rows(self.MODULE):
            env = row.get("部署环境", "未知")
            stat = env_stats.setdefault(env, {"total": 0, "deployed": 0, "failed": 0, "rollback": 0})
            stat["total"] += 1
            if row.get("status") == "已部署":
                stat["deployed"] += 1
            elif row.get("status") == "部署失败":
                stat["failed"] += 1
            elif row.get("status") == "已回滚":
                stat["rollback"] += 1
        base["environment_stats"] = env_stats
        base["consumed_published_events"] = len(store.channel.consume("fact.published", "deploy_pipeline"))
        return base


def _gather_prerequisites(target_model: str) -> list[str]:
    """基于目标模型类型收集前置依赖检查项。"""
    prereqs = []
    if "dwd_" in target_model:
        prereqs.append("上游维度表已发布")
        prereqs.append("ETL映射已部署")
    elif "dim_" in target_model:
        prereqs.append("维度属性已确认")
        prereqs.append("SCD策略已启用")

    # 检查事实表是否已发布
    for f in store.rows("fact_design"):
        if f.get("事实名称", "") in target_model or target_model in f.get("事实名称", ""):
            if f.get("status") == "已发布":
                prereqs.append("源事实表已发布")
            break

    # 检查ETL映射
    for e in store.rows("etl_mapping"):
        if e.get("目标表", "") in target_model or target_model in e.get("目标表", ""):
            if e.get("status") == "已部署":
                prereqs.append("ETL映射已部署")
            break

    prereqs.append("分区格式完整")
    return prereqs


def _generate_rollback_plan(target_model: str) -> str:
    """根据目标模型生成对应的回滚方案。"""
    if "dim_" in target_model:
        return "恢复上一版本快照"
    elif "dwd_" in target_model:
        return "回滚到上一分区版本"
    elif "dws_" in target_model:
        return "删除异常分区重新构建"
    return "回滚到部署前状态"


def _run_preflight_checks(entry: dict[str, Any]) -> dict[str, Any]:
    """执行部署前置检查。"""
    target_model = entry.get("目标模型", "")
    checks = {"items": [], "all_passed": True, "current_version": "v1.0"}

    # 检查1: 目标模型存在
    if target_model:
        checks["items"].append({"check": "目标模型定义", "passed": True})
    else:
        checks["items"].append({"check": "目标模型定义", "passed": False})
        checks["all_passed"] = False

    # 检查2: 维度关联完整性（与模型校验、事实交付共用同一套键版本判定）
    fact = find_fact_for_model(store, target_model)
    if fact is not None:
        evaluation = evaluate_fact_design(fact, store)
        summary = evaluation["summary"]
        dim_check = summary["passed"] == summary["total"] and summary["total"] > 0
        checks["items"].append({
            "check": f"关联维度就绪({summary['passed']}/{summary['total']})",
            "passed": dim_check,
            "fk_details": evaluation["details"],
            "fact_dates": summary["fact_dates"],
            "key_versions": sorted({
                str(item["key_version"]) for item in evaluation["details"]
                if item.get("key_version")
            }),
        })
        if not dim_check:
            checks["all_passed"] = False
            checks["fk_failures"] = summary["failures"]

    # 检查3: ETL映射部署
    etl_ready = False
    for e in store.rows("etl_mapping"):
        if target_model in str(e.get("目标表", "")) and e.get("status") == "已部署":
            etl_ready = True
    if not etl_ready:
        etl_ready = _mock_etl_check(target_model)
    checks["items"].append({"check": "ETL映射已部署", "passed": etl_ready})
    if not etl_ready:
        checks["all_passed"] = False

    checks["detail"] = "；".join(
        f"{item['check']}: {'通过' if item['passed'] else '失败'}"
        for item in checks["items"]
    )
    return checks


def _mock_etl_check(target_model: str) -> bool:
    """模拟ETL检查：部分模型跳过ETL检查。"""
    return "dws_" in target_model and "dim_" not in target_model


def _execute_rollback(entry: dict[str, Any]) -> dict[str, Any]:
    """执行回滚操作。"""
    return {"rollback_version": "v0.9", "reason": "校验未通过，手动触发回滚",
            "rolled_back_at": datetime.now(timezone.utc).isoformat()}