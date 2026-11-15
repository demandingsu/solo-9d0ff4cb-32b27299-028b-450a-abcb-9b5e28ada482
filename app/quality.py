"""监测站小时读数质量评估：整批 CSV 解析校验、组内对照评估、放行依据计算。

判定口径：
- 整批校验（任一违反即整批拒绝，保留原结果）：
  - 表头必须可映射到五个字段：观测ID、站点、UTC小时、分组、读数；
  - 观测 ID 全批唯一；同一站点同一 UTC 小时只允许一条记录；
  - 读数为有限数值；UTC 小时可解析且为整点。
- 对照评估：每条读数与同分组、同 UTC 小时的**其他站点**读数比较，
  - 参照站 ≥ 2：取参照读数中位数，|读数 - 中位数| > 绝对阈值 => 异常；
  - 参照站 < 2：参照不足 => 待补；
  - 其余 => 正常。
- 放行依据（basis）：对「阈值 + 本行全部字段 + 参与对照的全部记录
  （观测ID+站点+读数）」计算 SHA-256。重传整批后依据不变的放行继续有效，
  依据变化（本行变了、参与对照的记录增删改、阈值变了）则旧放行自动失效。
  放行操作必须提交页面当前看到的依据值，与现役批次不一致即拒绝（过期依据）。

本模块只依赖标准库，可脱离 FastAPI 单独测试。
"""
from __future__ import annotations

import csv
import hashlib
import io
import math
import statistics
from datetime import datetime, timezone
from typing import Any, Optional

NORMAL = "normal"    # 正常
ANOMALY = "anomaly"  # 异常（超阈值）
PENDING = "pending"  # 待补（参照不足）

STATUS_LABEL = {NORMAL: "正常", ANOMALY: "异常", PENDING: "待补"}

MIN_PEERS = 2          # 参与对照的其他站点最少数量
MAX_ROWS = 200_000     # 单批最大行数（防御性上限）


class FormatError(ValueError):
    """输入 CSV / 阈值不合法（整批拒绝，保留原结果）。"""


# ---------------------------------------------------------------------------
# 表头映射
# ---------------------------------------------------------------------------

def _norm_header(h: str) -> str:
    """表头归一化：去空白/下划线/连字符/冒号，英文小写。"""
    return "".join(ch for ch in h.strip().lower() if ch not in " _-:：")


FIELD_ALIASES: dict[str, set[str]] = {
    "obs_id": {"obsid", "observationid", "observation", "观测id", "观测编号", "记录id"},
    "station": {"station", "stationid", "站点", "站点id", "站号"},
    "hour": {"hour", "utchour", "小时", "utc小时", "观测小时"},
    "group": {"group", "groupid", "分组", "组别", "组"},
    "reading": {"reading", "value", "读数", "数值", "观测值"},
}

REQUIRED_FIELDS = list(FIELD_ALIASES)


# ---------------------------------------------------------------------------
# 字段解析
# ---------------------------------------------------------------------------

def parse_hour(raw: str, lineno: Optional[int] = None) -> str:
    """解析 UTC 小时并归一化为 'YYYY-MM-DDTHH:00Z'。非整点或无法解析则报错。"""
    where = f"第 {lineno} 行：" if lineno else ""
    s = (raw or "").strip()
    if not s:
        raise FormatError(f"{where}UTC 小时为空")
    iso = s[:-1] + "+00:00" if s.endswith(("Z", "z")) else s
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError as e:
        raise FormatError(
            f"{where}无法解析 UTC 小时 {raw!r}（示例：2026-11-15T13:00Z）") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)  # 未带时区按 UTC 处理
    dt = dt.astimezone(timezone.utc)
    if dt.minute or dt.second or dt.microsecond:
        raise FormatError(f"{where}{raw!r} 换算到 UTC 后不是整点小时")
    return dt.strftime("%Y-%m-%dT%H:00Z")


def parse_reading(raw: str, lineno: Optional[int] = None) -> float:
    where = f"第 {lineno} 行：" if lineno else ""
    s = (raw or "").strip()
    if not s:
        raise FormatError(f"{where}读数为空")
    try:
        v = float(s)
    except ValueError as e:
        raise FormatError(f"{where}读数 {raw!r} 不是数值") from e
    if not math.isfinite(v):
        raise FormatError(f"{where}读数 {raw!r} 不是有限数值")
    return v


def parse_threshold(raw: str) -> float:
    """绝对阈值：有限数值且 >= 0。"""
    s = (raw or "").strip()
    if not s:
        raise FormatError("请设置绝对阈值")
    try:
        v = float(s)
    except ValueError as e:
        raise FormatError(f"绝对阈值 {raw!r} 不是数值") from e
    if not math.isfinite(v) or v < 0:
        raise FormatError(f"绝对阈值 {raw!r} 必须是不小于 0 的有限数值")
    return v


# ---------------------------------------------------------------------------
# CSV 整批解析
# ---------------------------------------------------------------------------

def parse_csv(raw: bytes) -> list[dict[str, Any]]:
    """解析整批 CSV，返回记录列表。任何格式/唯一性问题都抛 FormatError（整批拒绝）。"""
    try:
        text = raw.decode("utf-8-sig")  # 容忍 BOM
    except UnicodeDecodeError as e:
        raise FormatError("CSV 必须是 UTF-8 编码") from e
    rows = [r for r in csv.reader(io.StringIO(text)) if any(c.strip() for c in r)]
    if not rows:
        raise FormatError("CSV 为空")

    header = rows[0]
    mapping: dict[int, str] = {}
    used: dict[str, str] = {}
    for idx, h in enumerate(header):
        key = _norm_header(h)
        if not key:
            raise FormatError(f"第 {idx + 1} 列表头为空")
        field = next((f for f, aliases in FIELD_ALIASES.items() if key in aliases), None)
        if field is None:
            raise FormatError(
                f"无法识别的列 {h!r}（需要五列：观测ID、站点、UTC小时、分组、读数）")
        if field in used:
            raise FormatError(f"列 {h!r} 与列 {used[field]!r} 重复映射到字段 {field}")
        used[field] = h
        mapping[idx] = field
    missing = [f for f in REQUIRED_FIELDS if f not in used]
    if missing:
        raise FormatError("缺少必需列：" + "、".join(missing))

    out: list[dict[str, Any]] = []
    seen_obs: dict[str, int] = {}
    seen_station_hour: dict[tuple[str, str], int] = {}
    for lineno, r in enumerate(rows[1:], start=2):
        if len(r) != len(header):
            raise FormatError(
                f"第 {lineno} 行字段数 {len(r)} 与表头 {len(header)} 不一致，整批拒绝")
        rec = {field: r[idx].strip() for idx, field in mapping.items()}
        obs_id, station, group = rec["obs_id"], rec["station"], rec["group"]
        if not obs_id:
            raise FormatError(f"第 {lineno} 行：观测 ID 为空，整批拒绝")
        if not station:
            raise FormatError(f"第 {lineno} 行：站点为空，整批拒绝")
        if not group:
            raise FormatError(f"第 {lineno} 行：分组为空，整批拒绝")
        hour = parse_hour(rec["hour"], lineno)
        reading = parse_reading(rec["reading"], lineno)
        if obs_id in seen_obs:
            raise FormatError(
                f"第 {lineno} 行：观测 ID {obs_id!r} 与第 {seen_obs[obs_id]} 行重复，整批拒绝")
        sh_key = (station, hour)
        if sh_key in seen_station_hour:
            raise FormatError(
                f"第 {lineno} 行：站点 {station!r} 在 {hour} 已有记录"
                f"（第 {seen_station_hour[sh_key]} 行），同站同小时重复，整批拒绝")
        seen_obs[obs_id] = lineno
        seen_station_hour[sh_key] = lineno
        out.append({"obs_id": obs_id, "station": station, "hour": hour,
                    "group": group, "reading": reading})
    if not out:
        raise FormatError("CSV 没有任何数据行")
    if len(out) > MAX_ROWS:
        raise FormatError(f"单批 {len(out)} 行超过上限 {MAX_ROWS}，整批拒绝")
    return out


# ---------------------------------------------------------------------------
# 放行依据
# ---------------------------------------------------------------------------

def basis_hash(row: dict[str, Any], peers: list[dict[str, Any]], threshold: float) -> str:
    """放行依据：阈值 + 本行全部字段 + 参与对照的记录（观测ID+站点+读数）。"""
    parts = [
        "v1",
        f"threshold={threshold!r}",
        f"self={row['obs_id']}|{row['station']}|{row['hour']}|{row['group']}|{row['reading']!r}",
    ]
    for p in sorted(peers, key=lambda x: x["obs_id"]):
        parts.append(f"peer={p['obs_id']}|{p['station']}|{p['reading']!r}")
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# 对照评估
# ---------------------------------------------------------------------------

def evaluate(rows: list[dict[str, Any]], threshold: float
             ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """对整批记录做组内对照评估，返回 (评估后的行, 汇总)。"""
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in rows:
        buckets.setdefault((r["group"], r["hour"]), []).append(r)

    out: list[dict[str, Any]] = []
    for r in rows:
        peers = [p for p in buckets[(r["group"], r["hour"])]
                 if p["station"] != r["station"]]
        median = statistics.median(p["reading"] for p in peers) if peers else None
        diff = abs(r["reading"] - median) if median is not None else None
        if len(peers) < MIN_PEERS:
            status = PENDING
        else:
            status = ANOMALY if diff > threshold else NORMAL
        out.append({
            **r,
            "peers": len(peers),
            "peer_obs": sorted(p["obs_id"] for p in peers),
            "median": median,
            "diff": diff,
            "status": status,
            "basis": basis_hash(r, peers, threshold),
        })

    summary = {
        "total": len(out),
        "normal": sum(1 for r in out if r["status"] == NORMAL),
        "anomaly": sum(1 for r in out if r["status"] == ANOMALY),
        "pending": sum(1 for r in out if r["status"] == PENDING),
        "threshold": threshold,
    }
    return out, summary


# ---------------------------------------------------------------------------
# 视图组装（放行状态 + 可信判定）
# ---------------------------------------------------------------------------

def build_view(payload: dict[str, Any],
               releases: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """把现役批次评估结果与放行记录合成查询视图。

    releases: {obs_id: {basis_hash, note, updated_at, batch_id}}
    放行有效 <=> 放行记录的 basis 与现役批次该读数的 basis 完全一致。
    """
    rows = []
    released_valid = trusted = 0
    for r in payload["rows"]:
        rel = releases.get(r["obs_id"])
        rel_view = None
        if rel is not None:
            valid = rel["basis_hash"] == r["basis"]
            rel_view = {"valid": valid, "note": rel["note"],
                        "at": rel["updated_at"], "batch_id": rel["batch_id"]}
        is_trusted = (r["status"] == NORMAL
                      or (r["status"] == ANOMALY and rel_view is not None and rel_view["valid"]))
        if rel_view and rel_view["valid"] and r["status"] == ANOMALY:
            released_valid += 1
        trusted += 1 if is_trusted else 0
        rows.append({**r, "release": rel_view, "trusted": is_trusted})
    summary = {**payload["summary"],
               "released_valid": released_valid,
               "trusted": trusted}
    return {"rows": rows, "summary": summary, "labels": STATUS_LABEL}


# ---------------------------------------------------------------------------
# 可信读数导出
# ---------------------------------------------------------------------------

DOWNLOAD_HEADER = ["观测ID", "站点", "UTC小时", "分组", "读数", "状态", "放行备注"]


def build_download_csv(view_rows: list[dict[str, Any]]) -> str:
    """导出可信读数（正常 + 有效放行的异常），调用方需先过滤 trusted。"""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(DOWNLOAD_HEADER)
    for r in view_rows:
        if r["status"] == NORMAL:
            qstatus, note = "正常", ""
        else:
            qstatus = "已放行"
            note = (r["release"] or {}).get("note", "")
        w.writerow([r["obs_id"], r["station"], r["hour"], r["group"],
                    repr(r["reading"]), qstatus, note])
    return buf.getvalue()
