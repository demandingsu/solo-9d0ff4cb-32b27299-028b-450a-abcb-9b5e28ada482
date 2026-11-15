"""FastAPI 入口：字段变更核查 + 小时读数质量放行，静态页面。"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from .evaluator import FormatError, build_report, parse_dependencies, parse_sources
from .quality import FormatError as QualityFormatError
from .quality import build_download_csv, evaluate, parse_csv, parse_threshold
from .store import (NoActiveBatchError, NotAnomalyError, ObsNotFoundError,
                    QualityStore, SnapshotStore, StaleBasisError)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
EXAMPLES_DIR = Path(os.environ.get("EXAMPLES_DIR", BASE_DIR.parent / "examples"))
DB_PATH = os.environ.get("DB_PATH", str(BASE_DIR.parent / "data" / "dwcheck.db"))

MAX_UPLOAD_BYTES = 20 * 1024 * 1024  # 单文件 20MB 上限

store = SnapshotStore(DB_PATH)
qstore = QualityStore(DB_PATH)
app = FastAPI(title="环境监测数仓 · 字段核查与读数质量放行", version="2.0.0")


async def _read_json(upload: UploadFile, label: str) -> Any:
    raw = await upload.read()
    if not raw:
        raise FormatError(f"{label}：文件为空")
    if len(raw) > MAX_UPLOAD_BYTES:
        raise FormatError(f"{label}：文件超过 20MB 上限")
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise FormatError(f"{label}：不是合法的 JSON 文件（{e}）") from e


def build_payload(name: str, sources_before_raw: Any, sources_after_raw: Any,
                  dependencies_raw: Any) -> dict:
    """解析 -> 评估 -> 组装自包含快照内容。任何 FormatError 都在此抛出。"""
    src_before = parse_sources(sources_before_raw)
    src_after = parse_sources(sources_after_raw)
    deps = parse_dependencies(dependencies_raw)
    report = build_report(deps, src_before, src_after)
    return {
        "name": name,
        "sources_before": sources_before_raw,
        "sources_after": sources_after_raw,
        "dependencies": dependencies_raw,
        "report": report,
    }


@app.post("/api/snapshots")
async def create_snapshot(
    sources_before: UploadFile = File(..., description="变更前源字段清单 JSON"),
    sources_after: UploadFile = File(..., description="变更后源字段清单 JSON"),
    dependencies: UploadFile = File(..., description="跨作业字段依赖 JSON"),
    name: str = Form(""),
):
    """上传三份清单并核查。成功则整版替换现役版本；失败保留旧版并留痕。"""
    snap_name = name.strip() or f"核查-{dependencies.filename or '未命名'}"
    try:
        payload = build_payload(
            snap_name,
            await _read_json(sources_before, "变更前源字段清单"),
            await _read_json(sources_after, "变更后源字段清单"),
            await _read_json(dependencies, "跨作业字段依赖"),
        )
    except FormatError as e:
        store.record_failed(snap_name, str(e))
        raise HTTPException(status_code=400, detail=str(e)) from e
    snap_id = store.publish(snap_name, payload)
    return {"id": snap_id, "name": snap_name, "summary": payload["report"]["summary"]}


@app.get("/api/snapshots/current")
async def get_current():
    """现役版本全量内容（查询只命中单一版本，绝不混版）。"""
    cur = store.current()
    if not cur:
        raise HTTPException(status_code=404, detail="暂无现役版本，请先上传核查")
    return cur


@app.get("/api/snapshots")
async def list_snapshots():
    return {"items": store.history()}


@app.get("/api/health")
async def health():
    return {"status": "up"}


# ---------------------------------------------------------------------------
# 小时读数质量放行
# ---------------------------------------------------------------------------

async def _read_csv(upload: UploadFile) -> bytes:
    raw = await upload.read()
    if not raw:
        raise QualityFormatError("CSV 文件为空")
    if len(raw) > MAX_UPLOAD_BYTES:
        raise QualityFormatError("CSV 文件超过 20MB 上限")
    return raw


@app.post("/api/quality/batches")
async def create_quality_batch(
    file: UploadFile = File(..., description="整批读数 CSV（观测ID、站点、UTC小时、分组、读数）"),
    threshold: str = Form(..., description="绝对阈值"),
    name: str = Form(""),
):
    """上传整批 CSV 并评估。同站同小时重复等任何问题都整批拒绝，保留原结果。"""
    batch_name = name.strip() or f"批次-{file.filename or '未命名'}"
    try:
        thr = parse_threshold(threshold)
        rows = parse_csv(await _read_csv(file))
        evaluated, summary = evaluate(rows, thr)
    except QualityFormatError as e:
        qstore.record_failed(batch_name, str(e))
        raise HTTPException(status_code=400, detail=str(e)) from e
    payload = {"name": batch_name, "threshold": thr, "rows": evaluated, "summary": summary}
    batch_id = qstore.publish_batch(batch_name, thr, payload)
    return {"id": batch_id, "name": batch_name, "summary": summary}


@app.get("/api/quality/current")
async def get_quality_current():
    """现役批次全量视图（评估 + 放行状态）。单事务读取，只对应一个已提交状态。"""
    cur = qstore.current()
    if not cur:
        raise HTTPException(status_code=404, detail="暂无现役批次，请先上传 CSV")
    return cur


@app.get("/api/quality/batches")
async def list_quality_batches():
    return {"items": qstore.history()}


class ReleaseIn(BaseModel):
    obs_id: str
    basis_hash: str
    note: str = ""


@app.post("/api/quality/releases")
async def create_release(body: ReleaseIn):
    """放行一条异常读数。依据过期（批次已更新）返回 409，需刷新后重试。"""
    try:
        return qstore.release(body.obs_id.strip(), body.basis_hash.strip(),
                              body.note.strip()[:500])
    except NoActiveBatchError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ObsNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except StaleBasisError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except NotAnomalyError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.get("/api/quality/download")
async def download_trusted():
    """下载可信读数 CSV：只含正常及有效放行的记录。"""
    cur = qstore.current()
    if not cur:
        raise HTTPException(status_code=404, detail="暂无现役批次，请先上传 CSV")
    trusted = [r for r in cur["rows"] if r["trusted"]]
    csv_text = build_download_csv(trusted)
    return Response(
        content="\ufeff" + csv_text,  # \ufeff 为 BOM，便于 Excel 识别 UTF-8
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition":
                 f'attachment; filename="trusted_readings_batch{cur["id"]}.csv"'})


EXAMPLE_FILES = {
    "sources_before.json": "application/json",
    "sources_after.json": "application/json",
    "dependencies.json": "application/json",
    "readings_example.csv": "text/csv; charset=utf-8",
}


@app.get("/examples/{filename}")
async def get_example(filename: str):
    media = EXAMPLE_FILES.get(filename)
    if media is None:
        raise HTTPException(status_code=404, detail="未知示例文件")
    path = EXAMPLES_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="示例文件未部署")
    return FileResponse(path, media_type=media, filename=filename)


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")
