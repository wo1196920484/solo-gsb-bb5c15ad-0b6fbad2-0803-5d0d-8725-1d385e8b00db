"""FastAPI 入口：启动即建表。"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query

from . import database, service
from .config import settings
from .schemas import (
    BatchIn,
    BatchResult,
    BatchSummary,
    MessageOut,
    RelinkIn,
    RelinkResult,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db()
    yield


app = FastAPI(
    title=settings.app_title,
    version="1.0.0",
    description="批量提交邮件头信息，归并为稳定排序的会话树，诊断重复 ID / 缺失引用 / 引用环。",
    lifespan=lifespan,
)


@app.get("/health", tags=["meta"], summary="健康检查")
def health() -> dict:
    return {"status": "ok"}


@app.post(
    "/batches",
    response_model=BatchResult,
    status_code=201,
    tags=["batches"],
    summary="提交一批邮件并归并会话",
)
def submit_batch(payload: BatchIn) -> dict:
    return service.submit_batch(payload)


@app.get(
    "/batches",
    tags=["batches"],
    summary="列出批次（按提交时间倒序）",
)
def list_batches(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> dict:
    return service.list_batches(limit, offset)


@app.get(
    "/batches/{batch_id}",
    response_model=BatchResult,
    tags=["batches"],
    summary="按批次 ID 查询归档结果（线程安全的快照）",
)
def get_batch(batch_id: str) -> dict:
    result = service.get_batch(batch_id)
    if result is None:
        raise HTTPException(status_code=404, detail="batch not found")
    return result


@app.post(
    "/relinks",
    response_model=RelinkResult,
    tags=["messages"],
    summary="迟到邮件归档后，批量修复此前断链的有效邮件",
)
def relink_messages(payload: RelinkIn) -> dict:
    try:
        return service.relink_messages(payload)
    except service.RelinkNotFound as exc:
        raise HTTPException(
            status_code=404,
            detail={"message": "message not found", "message_ids": exc.message_ids},
        )
    except service.RelinkCycleConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "relink would create a reference cycle; no changes were written",
                "cycles": exc.cycles,
            },
        )


@app.get(
    "/messages/{message_id}",
    response_model=MessageOut,
    tags=["messages"],
    summary="按 message_id 查询邮件及其最终父节点",
)
def get_message(message_id: str) -> dict:
    msg = service.get_message(message_id)
    if msg is None:
        raise HTTPException(status_code=404, detail="message not found")
    return msg


@app.get(
    "/threads/{message_id}",
    tags=["threads"],
    summary="查询某邮件所属的完整会话树",
)
def get_thread(message_id: str) -> dict:
    result = service.get_thread(message_id)
    if result is None:
        raise HTTPException(status_code=404, detail="message not found")
    if result.get("invalid"):
        raise HTTPException(
            status_code=409,
            detail="message belongs to a reference cycle and is not attached to any thread",
        )
    return result


@app.get(
    "/threads/{message_id}/page",
    tags=["threads"],
    summary="游标分页查询会话树（先序扁平展开，含 depth）",
)
def get_thread_page(
    message_id: str,
    limit: int = Query(50, ge=1, le=200, description="每页条数（1～200）"),
    cursor: Optional[str] = Query(None, description="上一页返回的 next_cursor；首次请求留空"),
) -> dict:
    try:
        result = service.get_thread_page(message_id, limit, cursor)
    except service.CursorMalformed:
        raise HTTPException(status_code=422, detail="cursor is malformed")
    except service.CursorThreadMismatch:
        raise HTTPException(
            status_code=422,
            detail="cursor does not belong to the thread of the path message_id",
        )
    except service.CursorStale:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "cursor_stale",
                "message": "the thread has changed since the cursor was issued; restart from the first page",
            },
        )
    if result is None:
        raise HTTPException(status_code=404, detail="message not found")
    if result.get("invalid"):
        raise HTTPException(
            status_code=409,
            detail="message belongs to a reference cycle and is not attached to any thread",
        )
    return result
