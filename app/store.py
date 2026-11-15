"""快照存储：核查结果整版入库，成功后原子替换现役版本，失败保留旧版。

- 每个快照自包含（源清单两版 + 依赖 + 报告），查询只读取 state='active' 的唯一版本，
  因此任何查询都不会混用两版数据。
- 新版本的解析/评估全部在写事务之外完成；替换仅一条事务内的
  UPDATE(archived) + INSERT(active)，失败回滚则旧版原样保留。
- WAL 模式保证核查（写入）进行时，旧版仍可被并发读取。

质量放行存储（QualityStore）共用同一数据库文件：
- quality_batches 整批自包含（评估后的全部行 + 汇总），同样整版原子替换；
- quality_releases 按观测 ID 记录放行（含放行依据哈希），跨批次保留，
  是否有效由查询时与现役批次的依据比对决定；
- 放行与上传交错时：放行在单条 BEGIN IMMEDIATE 事务内完成「读现役批次 ->
  校验依据 -> 写放行」，查询/下载在单条读事务内完成，均只对应一个已提交状态。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from .quality import ANOMALY, build_view

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    created_at TEXT NOT NULL,
    state      TEXT NOT NULL CHECK (state IN ('active', 'archived', 'failed')),
    error      TEXT,
    payload    TEXT
);
CREATE INDEX IF NOT EXISTS idx_snapshots_state ON snapshots(state, id);
"""


class SnapshotStore:
    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.Lock()
        conn = self._conn()
        try:
            conn.executescript(SCHEMA)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.commit()
        finally:
            conn.close()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    # -- 写 ---------------------------------------------------------------

    def publish(self, name: str, payload: dict[str, Any]) -> int:
        """整版替换：新快照置为 active，旧 active 置为 archived。事务失败则旧版保留。"""
        blob = json.dumps(payload, ensure_ascii=False)
        with self._lock:
            conn = self._conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("UPDATE snapshots SET state='archived' WHERE state='active'")
                cur = conn.execute(
                    "INSERT INTO snapshots(name, created_at, state, payload) VALUES (?,?, 'active', ?)",
                    (name, self._now(), blob))
                conn.commit()
                return int(cur.lastrowid)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def record_failed(self, name: str, error: str) -> int:
        """核查失败留痕，不影响现役版本。"""
        with self._lock:
            conn = self._conn()
            try:
                cur = conn.execute(
                    "INSERT INTO snapshots(name, created_at, state, error) VALUES (?,?, 'failed', ?)",
                    (name or "(未命名)", self._now(), error))
                conn.commit()
                return int(cur.lastrowid)
            finally:
                conn.close()

    # -- 读（只读现役版本，绝不混版） --------------------------------------

    def current(self) -> Optional[dict[str, Any]]:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT id, name, created_at, payload FROM snapshots "
                "WHERE state='active' ORDER BY id DESC LIMIT 1").fetchone()
            if not row:
                return None
            payload = json.loads(row["payload"])
            return {"id": row["id"], "name": row["name"],
                    "created_at": row["created_at"], "payload": payload}
        finally:
            conn.close()

    def history(self, limit: int = 20) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT id, name, created_at, state, error FROM snapshots "
                "ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# 质量放行：批次 + 放行
# ---------------------------------------------------------------------------

QUALITY_SCHEMA = """
CREATE TABLE IF NOT EXISTS quality_batches (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    created_at TEXT NOT NULL,
    threshold  REAL NOT NULL,
    state      TEXT NOT NULL CHECK (state IN ('active', 'archived', 'failed')),
    error      TEXT,
    payload    TEXT
);
CREATE INDEX IF NOT EXISTS idx_quality_batches_state ON quality_batches(state, id);

CREATE TABLE IF NOT EXISTS quality_releases (
    obs_id      TEXT PRIMARY KEY,
    basis_hash  TEXT NOT NULL,
    note        TEXT NOT NULL DEFAULT '',
    batch_id    INTEGER NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quality_release_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    obs_id     TEXT NOT NULL,
    basis_hash TEXT NOT NULL,
    note       TEXT NOT NULL DEFAULT '',
    batch_id   INTEGER,
    created_at TEXT NOT NULL
);
"""


class NoActiveBatchError(RuntimeError):
    """当前没有现役批次。"""


class ObsNotFoundError(RuntimeError):
    """观测 ID 不在现役批次中。"""


class StaleBasisError(RuntimeError):
    """放行依据与现役批次不一致（过期依据）。"""


class NotAnomalyError(RuntimeError):
    """目标读数当前不是异常状态，不能放行。"""


class QualityStore:
    """质量批次与放行存储。与 SnapshotStore 共用数据库文件，表互相独立。"""

    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.Lock()
        conn = self._conn()
        try:
            conn.executescript(QUALITY_SCHEMA)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.commit()
        finally:
            conn.close()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    # -- 写：整批 ----------------------------------------------------------

    def publish_batch(self, name: str, threshold: float, payload: dict[str, Any]) -> int:
        """整版替换：新批次置 active，旧 active 置 archived。失败回滚，旧版保留。

        放行记录（quality_releases）不在批次内，跨批次保留；
        新批次中依据已变化的读数，其旧放行在查询时自动失效。
        """
        blob = json.dumps(payload, ensure_ascii=False)
        with self._lock:
            conn = self._conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("UPDATE quality_batches SET state='archived' WHERE state='active'")
                cur = conn.execute(
                    "INSERT INTO quality_batches(name, created_at, threshold, state, payload) "
                    "VALUES (?,?,?, 'active', ?)",
                    (name, self._now(), threshold, blob))
                conn.commit()
                return int(cur.lastrowid)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def record_failed(self, name: str, error: str) -> int:
        """上传失败留痕，不影响现役批次。"""
        with self._lock:
            conn = self._conn()
            try:
                cur = conn.execute(
                    "INSERT INTO quality_batches(name, created_at, threshold, state, error) "
                    "VALUES (?,?, 0, 'failed', ?)",
                    (name or "(未命名)", self._now(), error))
                conn.commit()
                return int(cur.lastrowid)
            finally:
                conn.close()

    # -- 写：放行 ----------------------------------------------------------

    def release(self, obs_id: str, basis_hash: str, note: str) -> dict[str, Any]:
        """放行一条异常读数。

        单条 BEGIN IMMEDIATE 事务内：读现役批次 -> 校验依据未过期且状态为异常
        -> 写放行。与并发上传互斥，校验所依据的状态即提交时的已提交状态。
        """
        with self._lock:
            conn = self._conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                brow = conn.execute(
                    "SELECT id, name, payload FROM quality_batches "
                    "WHERE state='active' ORDER BY id DESC LIMIT 1").fetchone()
                if not brow:
                    raise NoActiveBatchError("暂无现役批次，请先上传 CSV")
                payload = json.loads(brow["payload"])
                row = next((r for r in payload["rows"] if r["obs_id"] == obs_id), None)
                if row is None:
                    raise ObsNotFoundError(
                        f"观测 ID {obs_id!r} 不在现役批次 #{brow['id']} 中")
                if row["basis"] != basis_hash:
                    raise StaleBasisError(
                        f"放行依据已过期：{obs_id!r} 在现役批次 #{brow['id']} 中的"
                        "对照依据已变化，请刷新页面后重新放行")
                if row["status"] != ANOMALY:
                    raise NotAnomalyError(
                        f"读数 {obs_id!r} 当前状态为「{row['status']}」，仅异常读数可放行")
                now = self._now()
                conn.execute(
                    "INSERT INTO quality_releases(obs_id, basis_hash, note, batch_id,"
                    " created_at, updated_at) VALUES (?,?,?,?,?,?) "
                    "ON CONFLICT(obs_id) DO UPDATE SET basis_hash=excluded.basis_hash,"
                    " note=excluded.note, batch_id=excluded.batch_id,"
                    " updated_at=excluded.updated_at",
                    (obs_id, basis_hash, note, brow["id"], now, now))
                conn.execute(
                    "INSERT INTO quality_release_log(obs_id, basis_hash, note, batch_id,"
                    " created_at) VALUES (?,?,?,?,?)",
                    (obs_id, basis_hash, note, brow["id"], now))
                conn.commit()
                return {"obs_id": obs_id, "batch_id": brow["id"], "released_at": now}
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    # -- 读（单条读事务，只对应一个已提交状态） ------------------------------

    def current(self) -> Optional[dict[str, Any]]:
        """现役批次 + 放行状态的合成视图。批次与放行在同一读事务内读取。"""
        conn = self._conn()
        try:
            conn.execute("BEGIN")
            brow = conn.execute(
                "SELECT id, name, created_at, threshold, payload FROM quality_batches "
                "WHERE state='active' ORDER BY id DESC LIMIT 1").fetchone()
            if not brow:
                conn.rollback()
                return None
            rels = conn.execute(
                "SELECT obs_id, basis_hash, note, batch_id, updated_at "
                "FROM quality_releases").fetchall()
            conn.commit()
            payload = json.loads(brow["payload"])
            releases = {r["obs_id"]: dict(r) for r in rels}
            view = build_view(payload, releases)
            return {"id": brow["id"], "name": brow["name"],
                    "created_at": brow["created_at"], "threshold": brow["threshold"],
                    **view}
        finally:
            conn.close()

    def history(self, limit: int = 20) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT id, name, created_at, threshold, state, error "
                "FROM quality_batches ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
