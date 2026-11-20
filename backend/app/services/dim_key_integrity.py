"""维度键完整性统一引擎：事实外键、维度键版本和分区交付共用同一份明细。

设计约束：
- 外键校验不再只扫描维度定义是否存在，而是按事实日期核对键版本是否处于生效期；
- 已停用、跨生效期、未确认版本以及完全不存在的键都计入失败；
- 每条明细都记录事实日期、采用的键版本和当前键版本，方便维度换键后回溯；
- 通过率严格由同一批明细计算，重复校验逐分区重算，不跳过跨版本边界的分区；
- 事实设计、模型校验和分区交付都调用本模块，避免各处分别维护“有效键”规则。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

# 外键明细状态 ----------------------------------------------------------------
VALID = "valid"
UNKNOWN = "unknown"
DEACTIVATED = "deactivated"
OUTSIDE_EFFECTIVE_PERIOD = "outside_effective_period"
UNVERIFIED = "unverified"
MISSING = "missing"

STATUS_LABELS = {
    VALID: "键有效",
    UNKNOWN: "键不存在",
    DEACTIVATED: "键已停用",
    OUTSIDE_EFFECTIVE_PERIOD: "跨生效期",
    UNVERIFIED: "键版本未确认",
    MISSING: "外键缺失",
}

DEACTIVATED_STATUSES = {"已停用", "已失效", "已下线", "已废止", "已归档"}
CONFIRMED_STATUSES = {"已发布", "已生效"}

MAX_DATE = date(9999, 12, 31)
DEFAULT_DATE_FIELDS = ("事实日期", "业务日期", "dt", "ds", "分区值")

_DATE_RE = re.compile(r"(\d{4})[-/.]?(\d{1,2})[-/.]?(\d{1,2})")


def parse_fact_date(value: Any) -> date | None:
    """把分区值、日期字段或时间戳解析为日期；无法解析时返回 None。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    match = _DATE_RE.search(text)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def split_dim_codes(value: Any) -> list[str]:
    """拆分关联维度字符串，兼容点号、逗号和空白分隔。"""
    if value is None:
        return []
    return [part.strip() for part in re.split(r"[.,，、\s]+", str(value)) if part.strip()]


def _as_date(value: Any, default: date) -> date:
    parsed = parse_fact_date(value)
    return parsed if parsed is not None else default


@dataclass(frozen=True)
class DimensionKeyVersion:
    """一条维度键版本：由 dim_definition 中的一行描述。"""

    dim_code: str
    version_id: int | str | None
    status: str
    effective_from: date
    effective_to: date
    current: bool
    raw: Mapping[str, Any]

    @property
    def confirmed(self) -> bool:
        return self.status in CONFIRMED_STATUSES

    @property
    def deactivated(self) -> bool:
        # 只有显式停用的键版本才算停用；SCD2 的历史版本仍然在它自己的
        # 生效期内可用，不能因为 current_marker=0 就判为停用。
        return self.status in DEACTIVATED_STATUSES

    def covers(self, fact_date: date | None) -> bool:
        if fact_date is None:
            return True
        return self.effective_from <= fact_date <= self.effective_to

    def member_keys(self) -> set[str]:
        """这个键版本里已确认存在的键值。

        维度定义的每一行既描述一个键版本，也在“键值”列里列出该版本下
        实际存在的键。未维护键值列时退化为维度编码本身，保持与设计级
        校验一致。
        """
        raw_keys = self.raw.get("键值") or self.raw.get("维度键值") or self.raw.get("成员键")
        if isinstance(raw_keys, (list, tuple, set)):
            members = {str(key).strip() for key in raw_keys if str(key).strip()}
        elif raw_keys:
            members = set(split_dim_codes(raw_keys))
        else:
            members = set()
        return members or {self.dim_code}

    def has_key(self, key_value: str) -> bool:
        return key_value in self.member_keys()

    def version_label(self) -> str:
        return f"{self.dim_code}#{self.version_id}"


def _build_versions(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[DimensionKeyVersion]]:
    versions: dict[str, list[DimensionKeyVersion]] = {}
    for row in rows:
        dim_code = str(row.get("维度编码", "")).strip()
        if not dim_code:
            continue
        # 未显式维护版本区间时，以当前键的开放区间兜底。
        current = str(row.get("current_marker", "1")) in {"1", "Y", "y", "true", "True"}
        raw_from = row.get("生效时间")
        raw_to = row.get("失效时间")
        versions.setdefault(dim_code, []).append(
            DimensionKeyVersion(
                dim_code=dim_code,
                version_id=row.get("id"),
                status=str(row.get("status", "")),
                effective_from=_as_date(raw_from, date.min),
                effective_to=_as_date(raw_to, MAX_DATE),
                current=current,
                raw=row,
            )
        )
    for dim_versions in versions.values():
        dim_versions.sort(key=lambda item: (item.effective_from, str(item.version_id)))
    return versions


class DimensionKeyIndex:
    """按维度编码索引全部键版本，回答“某个键在某个事实日期是否可用”。"""

    def __init__(self, dim_rows: Sequence[Mapping[str, Any]] | None = None) -> None:
        self._versions = _build_versions(dim_rows or [])

    @classmethod
    def from_store(cls, store: Any, dim_module: str = "dim_definition") -> "DimensionKeyIndex":
        return cls(store.rows(dim_module))

    def known(self, dim_code: str) -> bool:
        return bool(self._versions.get(dim_code))

    def versions(self, dim_code: str) -> list[DimensionKeyVersion]:
        return list(self._versions.get(dim_code, []))

    def current_version(self, dim_code: str) -> DimensionKeyVersion | None:
        versions = self._versions.get(dim_code, [])
        for version in reversed(versions):
            if version.current and not version.deactivated:
                return version
        return versions[-1] if versions else None

    def settled_version(self, dim_code: str) -> DimensionKeyVersion:
        """挑一个“该维度键当前在用”的代表版本，用于生成设计级样本。"""
        versions = self._versions.get(dim_code, [])
        if not versions:
            raise KeyError(dim_code)
        current = self.current_version(dim_code)
        if current is not None and current.confirmed and not current.deactivated:
            return current
        for version in reversed(versions):
            if version.confirmed and not version.deactivated:
                return version
        return versions[-1]

    def classify(
        self,
        dim_code: str,
        fact_date: date | None,
        key_value: str | None = None,
    ) -> tuple[str, DimensionKeyVersion | None]:
        """判断某个外键值在事实日期下是否可用。

        返回 (状态, 命中的键版本)。判定分三层：
        1. 外键值是否存在于该维度的某个键版本里（否则为“键不存在”）；
        2. 事实日期是否落在某个已确认版本的生效期内（否则为“跨生效期”）；
        3. 命中的版本是否已停用或尚未确认。

        状态为 valid 时才会返回命中的版本；其它状态返回的版本仅用于
        在明细里展示事实日期旁边的参考键版本。
        """
        versions = self._versions.get(dim_code, [])
        if not versions:
            return UNKNOWN, None
        key_value = (key_value or "").strip()

        # 事实日期缺失时，只能核对该键当前是否仍处于已确认状态；
        # 没有可用版本不等于“键不存在”，而是无法确认版本。
        if fact_date is None:
            current = self.current_version(dim_code)
            if key_value and not any(v.has_key(key_value) for v in versions):
                return UNKNOWN, current
            if current is not None and current.confirmed and not current.deactivated:
                return VALID, current
            if current is not None and current.deactivated:
                return DEACTIVATED, current
            return UNVERIFIED, current

        current = self.current_version(dim_code)

        # 先按键值筛版本：外键值必须确实出现在某个版本的成员键里。
        known_versions = versions
        if key_value:
            known_versions = [v for v in versions if v.has_key(key_value)]
            if not known_versions:
                return UNKNOWN, current

        covering = [version for version in known_versions if version.covers(fact_date)]

        confirmed_covering = [v for v in covering if v.confirmed and not v.deactivated]
        if confirmed_covering:
            return VALID, confirmed_covering[-1]

        # 事实日期落在某个版本区间内，但该版本已停用或尚未确认。
        if covering:
            candidate = covering[-1]
            if candidate.deactivated:
                return DEACTIVATED, candidate
            return UNVERIFIED, candidate

        # 键值存在于维度里，但事实日期落在所有相关版本区间之外。
        # 如果换键之前的版本仍然已确认（例如 SCD2 历史行），问题不是“旧键
        # 失效”，而是新旧标准冲突：事实日期仍然落在旧键有效期内，但没有
        # 任何版本声明它属于哪一边，因此按“跨生效期”报告。
        if any(v.confirmed and not v.deactivated for v in known_versions):
            reference = max(
                (v for v in known_versions if v.confirmed and not v.deactivated),
                key=lambda v: v.effective_to,
            )
            return OUTSIDE_EFFECTIVE_PERIOD, reference
        reference = max(known_versions, key=lambda v: v.effective_to)
        if reference.deactivated:
            return DEACTIVATED, reference
        return OUTSIDE_EFFECTIVE_PERIOD, reference


def fact_date_fields(partition_field: str | None = None) -> tuple[str, ...]:
    fields: list[str] = []
    if partition_field:
        name = str(partition_field).strip().split()[0] if str(partition_field).strip() else ""
        if name:
            fields.append(name.split("=")[-1])
    for candidate in DEFAULT_DATE_FIELDS:
        if candidate not in fields:
            fields.append(candidate)
    return tuple(fields)


def resolve_fact_date(record: Mapping[str, Any], partition_field: str | None = None) -> tuple[date | None, str]:
    """从事实记录中取事实日期，返回 (日期, 来源字段)。"""
    for field in fact_date_fields(partition_field):
        if field in record:
            parsed = parse_fact_date(record.get(field))
            if parsed is not None:
                return parsed, field
    return None, ""


def _next_version_id(rows: Sequence[Mapping[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def refill_missing_keys(
    store: Any,
    dim_code: str,
    fact_date: date | None,
    *,
    key_value: str | None = None,
    dim_module: str = "dim_definition",
) -> dict[str, Any]:
    """为事实日期补一个可用的键版本。

    如果该日期已有已确认版本，则直接复用；如果被已停用版本占住区间，
    则补一条新的已发布版本，生效期从事实日期开始。补出来的版本会继承
    原版本的键值集合，并把本次事实用到的键并入；如果外键值从未出现在
    该维度里，则说明是脏键，补版本解决不了，交由调用方拦截。
    """
    rows = store.rows(dim_module)
    index = DimensionKeyIndex(rows)
    if not index.known(dim_code):
        return {"dim_code": dim_code, "refilled": False, "reason": UNKNOWN}

    key_value = (key_value or "").strip()
    status, hit = index.classify(dim_code, fact_date, key_value or None)
    if status == VALID:
        return {
            "dim_code": dim_code,
            "refilled": False,
            "reason": VALID,
            "version_id": hit.version_id if hit else None,
        }
    if status == UNKNOWN:
        # 外键值根本不在该维度里，补键只会掩盖脏数据。
        return {"dim_code": dim_code, "refilled": False, "reason": UNKNOWN, "value": key_value}

    template = hit or index.current_version(dim_code)
    now = datetime.now(timezone.utc)
    effective_from = fact_date or now.date()
    new_row: dict[str, Any] = {
        "id": _next_version_id(rows),
        "status": "已发布",
        "pending": False,
        "abnormal": False,
        "维度编码": dim_code,
        "current_marker": "1",
        "生效时间": effective_from.isoformat(),
        "失效时间": "9999-12-31",
    }
    if template is not None:
        for key, value in template.raw.items():
            new_row.setdefault(key, value)
        members = template.member_keys()
        if key_value:
            members.add(key_value)
        # 只有原始行本来就维护键值列时才写回，避免凭空造出键值列。
        if any(field in template.raw for field in ("键值", "维度键值", "成员键")):
            new_row["键值"] = ".".join(sorted(members))
        new_row["id"] = _next_version_id(rows)
        new_row["维度编码"] = dim_code
        new_row["current_marker"] = "1"
        new_row["status"] = "已发布"
        new_row["生效时间"] = effective_from.isoformat()
        new_row["失效时间"] = "9999-12-31"
        new_row["pending"] = False
        new_row["abnormal"] = False
        new_row["_refilled_from"] = template.version_id
        new_row["_refilled_at"] = now.isoformat()
    rows.append(new_row)
    return {
        "dim_code": dim_code,
        "refilled": True,
        "reason": status,
        "version_id": new_row["id"],
        "effective_from": new_row["生效时间"],
        "key_value": key_value,
    }


def fk_detail(
    *,
    fact_key: str,
    field: str,
    dim_code: str,
    value: Any,
    partition_value: str,
    fact_date: date | None,
    date_source: str,
    index: DimensionKeyIndex,
    record_index: int | None = None,
) -> dict[str, Any]:
    """构造一条外键完整性明细；所有明细后续都从这里派生统计。"""
    key_value = "" if value is None else str(value).strip()
    status: str
    hit: DimensionKeyVersion | None
    if not key_value:
        status, hit = MISSING, index.current_version(dim_code)
    else:
        status, hit = index.classify(dim_code, fact_date, key_value)

    current = index.current_version(dim_code)
    key_version = hit.version_label() if hit is not None else ""
    current_version = current.version_label() if current is not None else ""
    detail = {
        "fact_key": fact_key,
        "field": field,
        "dim_code": dim_code,
        "value": key_value,
        "partition": partition_value,
        "fact_date": fact_date.isoformat() if fact_date else "",
        "date_source": date_source,
        "scope": "record",
        "status": status,
        "status_label": STATUS_LABELS[status],
        "key_version": key_version,
        "current_key_version": current_version,
        "passes": status == VALID,
    }
    if record_index is not None:
        detail["record_index"] = record_index
    if status == VALID and hit is not None:
        detail["effective_from"] = hit.effective_from.isoformat()
        detail["effective_to"] = hit.effective_to.isoformat()
    return detail


def dimension_key_detail(
    *,
    scope_key: str,
    dim_code: str,
    partition_value: str,
    fact_date: date | None,
    date_source: str,
    index: DimensionKeyIndex,
) -> dict[str, Any]:
    """设计级明细：还没有分区记录时，只核对维度键在事实日期是否可用。

    这里不再允许“定义存在即通过”，而是使用和分区记录完全相同的版本区间判定。
    """
    fact_date = parse_fact_date(fact_date)
    status, hit = index.classify(dim_code, fact_date)
    current = index.current_version(dim_code)
    detail = {
        "fact_key": scope_key,
        "field": "维度键",
        "dim_code": dim_code,
        "value": dim_code,
        "partition": partition_value,
        "fact_date": fact_date.isoformat() if fact_date else "",
        "date_source": date_source,
        "scope": "design",
        "status": status,
        "status_label": STATUS_LABELS[status],
        "key_version": hit.version_label() if hit is not None else "",
        "current_key_version": current.version_label() if current is not None else "",
        "passes": status == VALID,
    }
    if status == VALID and hit is not None:
        detail["effective_from"] = hit.effective_from.isoformat()
        detail["effective_to"] = hit.effective_to.isoformat()
    return detail


def _partition_of(record: Mapping[str, Any], partition_field: str | None) -> str:
    if partition_field:
        name = str(partition_field).strip().split()[0]
        name = name.split("=")[-1]
        if name in record and str(record.get(name, "")).strip():
            return str(record.get(name)).strip()
    for fallback in ("分区值", "dt", "ds"):
        if fallback in record and str(record.get(fallback, "")).strip():
            return str(record.get(fallback)).strip()
    return "default"


def check_fk_group(
    *,
    fact_key: str,
    records: Sequence[Mapping[str, Any]],
    dim_fk_fields: Mapping[str, str],
    partition_field: str | None,
    index: DimensionKeyIndex,
    start_record_index: int = 0,
) -> list[dict[str, Any]]:
    """对一批记录逐条逐字段产出外键明细，不写库、不做去重。"""
    details: list[dict[str, Any]] = []
    for offset, record in enumerate(records):
        fact_date, date_source = resolve_fact_date(record, partition_field)
        partition_value = _partition_of(record, partition_field)
        for field, dim_code in dim_fk_fields.items():
            details.append(
                fk_detail(
                    fact_key=fact_key,
                    field=field,
                    dim_code=dim_code,
                    value=record.get(field),
                    partition_value=partition_value,
                    fact_date=fact_date,
                    date_source=date_source,
                    index=index,
                    record_index=start_record_index + offset,
                )
            )
    return details


def summarize(
    details: Sequence[Mapping[str, Any]],
    *,
    fact_key: str = "",
    fact_dates: Sequence[str] | None = None,
) -> dict[str, Any]:
    """从同一批明细计算通过率、状态分桶和版本边界信息。"""
    total = len(details)
    passed = sum(1 for item in details if item.get("passes"))
    status_counts: dict[str, int] = {}
    for status in STATUS_LABELS:
        status_counts[status] = 0
    for item in details:
        status = str(item.get("status", UNKNOWN))
        status_counts[status] = status_counts.get(status, 0) + 1

    partitions: dict[str, dict[str, Any]] = {}
    for item in details:
        partition = str(item.get("partition", "default"))
        bucket = partitions.setdefault(
            partition,
            {"total": 0, "passed": 0, "failed": 0, "fact_dates": set(), "statuses": {}},
        )
        bucket["total"] += 1
        if item.get("passes"):
            bucket["passed"] += 1
        else:
            bucket["failed"] += 1
        if item.get("fact_date"):
            bucket["fact_dates"].add(str(item["fact_date"]))
        status = str(item.get("status", UNKNOWN))
        bucket["statuses"][status] = bucket["statuses"].get(status, 0) + 1

    for bucket in partitions.values():
        bucket["fact_dates"] = sorted(bucket["fact_dates"])

    dates = sorted({str(item["fact_date"]) for item in details if item.get("fact_date")})
    if fact_dates:
        dates = sorted(set(dates) | {str(d) for d in fact_dates if d})

    failures = [item for item in details if not item.get("passes")]
    # 版本边界：所有版本命中与当前版本不同的事实日期，提示这里跨过了换键点。
    version_boundaries = sorted({
        str(item["fact_date"])
        for item in details
        if item.get("fact_date")
        and item.get("key_version")
        and item.get("current_key_version")
        and item["key_version"] != item["current_key_version"]
    })
    return {
        "fact_key": fact_key,
        "fact_dates": dates,
        "total": total,
        "passed": passed,
        "failed": total - passed,
        "pass_rate": f"{passed}/{total}" if total else "0/0",
        "status_counts": status_counts,
        "partitions": partitions,
        "version_boundaries": version_boundaries,
        "failures": failures,
    }


def evaluate_records(
    *,
    fact_key: str,
    records: Sequence[Mapping[str, Any]],
    dim_fk_fields: Mapping[str, str],
    partition_field: str | None,
    index: DimensionKeyIndex,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """校验一批记录的维度外键，返回明细和同一批明细算出的汇总。"""
    details = check_fk_group(
        fact_key=fact_key,
        records=records,
        dim_fk_fields=dim_fk_fields,
        partition_field=partition_field,
        index=index,
    )
    return details, summarize(details, fact_key=fact_key)


EN_ZH_HINTS = {
    "sales": ("销售",),
    "warehouse": ("仓储", "仓库"),
    "inventory": ("仓储", "库存"),
    "customer": ("客户",),
    "purchase": ("采购",),
    "order": ("订单",),
    "behavior": ("行为",),
    "channel": ("渠道",),
    "store": ("门店",),
    "product": ("产品",),
    "supplier": ("供应商",),
    "return": ("退货",),
    "refund": ("退款",),
    "marketing": ("营销",),
    "campaign": ("活动",),
    "time": ("时间",),
    "date": ("日期",),
    "detail": ("明细",),
    "change": ("变动",),
    "summary": ("汇总",),
}


def find_fact_for_model(
    store: Any,
    target_model: str,
    *,
    fact_module: str = "fact_design",
) -> dict[str, Any] | None:
    """把 dwd_/dim_ 形式的模型名解析回事实设计记录。

    先做名称子串匹配，再用业务词根对齐（dwd_sales_detail → 销售明细事实）。
    匹配不到时不猜测，交由调用方判定“无法确认外键”。
    """
    target = target_model.replace("dwd_", "").replace("dim_", "").lower()
    candidates = [
        row for row in store.rows(fact_module)
        if not row.get("_partition_record")
    ]
    if target:
        for row in candidates:
            fact_name = str(row.get("事实名称", "")).lower()
            if target in fact_name or (fact_name and fact_name in target):
                return row

    tokens = [token for token in target.replace("-", "_").split("_") if token]
    best: tuple[int, dict[str, Any] | None] = (0, None)
    for row in candidates:
        fact_name = str(row.get("事实名称", ""))
        score = sum(
            1 for token in tokens
            if any(hint in fact_name for hint in EN_ZH_HINTS.get(token, ()))
        )
        if score > best[0]:
            best = (score, row)
    return best[1] if best[0] > 0 else None


def fact_dimension_fields(
    fact: Mapping[str, Any],
    index: DimensionKeyIndex,
) -> dict[str, str]:
    """把事实设计的“关联维度”展开成 {外键字段: 维度编码}。

    关联维度默认以维度定义的主键列作为外键字段；没有主键定义时回退到
    维度编码本身，保证校验明细能定位到具体外键列。
    """
    fields: dict[str, str] = {}
    for dim_code in split_dim_codes(fact.get("关联维度", "")):
        versions = index.versions(dim_code)
        key_definition = ""
        if versions:
            key_definition = str(versions[-1].raw.get("主键定义", "")).strip()
        field = key_definition.split()[0].split("=")[-1] if key_definition else dim_code.lower()
        fields[field] = dim_code
    return fields


def _split_failed_by_date(details: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in details:
        if item.get("passes"):
            continue
        day = str(item.get("fact_date") or "未知日期")
        counts[day] = counts.get(day, 0) + 1
    return counts


def diff_foreign_keys(
    previous: Sequence[Mapping[str, Any]],
    current: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """比对两次校验的实际外键；重算覆盖全部分区，只报告真正变化的键。"""
    def key_of(item: Mapping[str, Any]) -> tuple[Any, Any, Any]:
        return (item.get("record_index"), item.get("field"), item.get("partition"))

    before = {key_of(item): item for item in previous}
    changes: list[dict[str, Any]] = []
    for item in current:
        old = before.get(key_of(item))
        if old is None:
            continue
        if old.get("value") == item.get("value") and old.get("status") == item.get("status"):
            continue
        changes.append({
            "record_index": item.get("record_index"),
            "partition": item.get("partition"),
            "field": item.get("field"),
            "dim_code": item.get("dim_code"),
            "fact_date": item.get("fact_date"),
            "old_value": old.get("value"),
            "new_value": item.get("value"),
            "old_status": old.get("status"),
            "new_status": item.get("status"),
            "key_version": item.get("key_version"),
        })
    return changes


def dimension_status(
    store: Any,
    dim_code: str,
    fact_date: Any = None,
    *,
    dim_module: str = "dim_definition",
) -> tuple[str, DimensionKeyVersion | None]:
    """统一的维度键可用性判定入口，供事实设计、校验和部署共用。"""
    index = DimensionKeyIndex.from_store(store, dim_module)
    return index.classify(dim_code, parse_fact_date(fact_date))


def collect_stored_fact_records(
    store: Any,
    fact_name: str,
    *,
    exclude_id: int | None = None,
    fact_module: str = "fact_design",
) -> list[dict[str, Any]]:
    """取出已经写入事实表的分区记录，用于重复校验同一批记录。"""
    records: list[dict[str, Any]] = []
    for row in store.rows(fact_module):
        if not row.get("_partition_record"):
            continue
        if str(row.get("事实名称", "")) != fact_name:
            continue
        if exclude_id is not None and int(row.get("id", 0)) == int(exclude_id):
            continue
        records.append(dict(row))
    return records


def synthesize_design_records(
    fact: Mapping[str, Any],
    dim_fk_fields: Mapping[str, str],
    fact_date: date | None,
    index: DimensionKeyIndex | None = None,
) -> list[dict[str, Any]]:
    """没有分区记录时，用事实设计的维度关联生成一条校验样本。

    样本里每个外键取该维度已确认版本里的一个真实键值，而不是维度编码：
    维度编码只证明“维度定义存在”，真实键值才能核对到具体键版本。事实
    日期决定采用哪个版本。这样即便还没有事实数据，也能发现已停用或未
    确认的维度。
    """
    index = index or DimensionKeyIndex()
    record: dict[str, Any] = {"分区值": "设计样本"}
    for field, dim_code in dim_fk_fields.items():
        key_value = dim_code
        if index.known(dim_code):
            try:
                version = index.settled_version(dim_code)
            except KeyError:
                version = None
            if version is not None:
                members = sorted(version.member_keys())
                key_value = members[0] if members else dim_code
        record[field] = key_value
    if fact_date is not None:
        record["事实日期"] = fact_date.isoformat()
    return [record]


def evaluate_fact_design(
    fact: Mapping[str, Any],
    store: Any,
    *,
    records: Sequence[Mapping[str, Any]] | None = None,
    fact_date: Any = None,
    previous_details: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """按事实设计校验维度外键，返回明细和同一批记录算出的汇总。

    优先使用显式传入或已经落库的分区记录；两者都没有时退化为设计级明细，
    逐维度核对键版本，不会因为“维度定义存在”就直接判为通过。
    """
    index = DimensionKeyIndex.from_store(store)
    fact_key = str(fact.get("事实名称", ""))
    dim_fk_fields = fact_dimension_fields(fact, index)
    partition_field = str(fact.get("分区字段", "") or "").strip().split()[0]
    fact_date_value = parse_fact_date(fact_date)

    if records is None:
        records = collect_stored_fact_records(store, fact_key)
    if records:
        details, summary = evaluate_records(
            fact_key=fact_key,
            records=records,
            dim_fk_fields=dim_fk_fields,
            partition_field=partition_field or None,
            index=index,
        )
        scope = "partition"
    else:
        details = [
            dimension_key_detail(
                scope_key=fact_key,
                dim_code=dim_code,
                partition_value="设计校验",
                fact_date=fact_date_value,
                date_source="规则事实日期" if fact_date_value else "未声明事实日期",
                index=index,
            )
            for dim_code in split_dim_codes(fact.get("关联维度", ""))
        ]
        summary = summarize(details, fact_key=fact_key)
        scope = "design"

    changes = diff_foreign_keys(list(previous_details or []), details)
    return {
        "fact_key": fact_key,
        "scope": scope,
        "dim_fields": dim_fk_fields,
        "partition_field": partition_field,
        "details": details,
        "summary": summary,
        "changed_foreign_keys": changes,
    }

