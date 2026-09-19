"""业务编排：调用纯归并逻辑，落库 SQLite，组装查询结果。"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional

from . import database, threading
from .schemas import BatchIn, CleanupIn, FreezeIn, RelinkIn, UnfreezeIn
from .threading import RetentionCleanupPlan, ThreadMessage


class RelinkNotFound(Exception):
    """POST /relinks：请求中存在未归档的 message_id。"""

    def __init__(self, message_ids: list[str]) -> None:
        self.message_ids = message_ids
        super().__init__(f"messages not found: {message_ids}")


class RelinkCycleConflict(Exception):
    """POST /relinks：合并候选关系后出现环，整次拒绝、不写库。"""

    def __init__(self, cycles: list[list[str]]) -> None:
        self.cycles = cycles
        super().__init__(f"relink would create cycles: {cycles}")


class CursorMalformed(Exception):
    """GET /threads/{id}/page：游标格式无法解析或字段非法（-> 422）。"""


class CursorThreadMismatch(Exception):
    """游标根 ID 与路径所属会话不是同一会话（-> 422）。"""


class CursorStale(Exception):
    """游标 revision / 根归属与当前树不一致（树在补链中变化，-> 409）。"""


class RetentionAnchorNotFound(Exception):
    """冻结/解冻锚点邮件未归档（-> 404）。"""


class RetentionAnchorInvalid(Exception):
    """冻结锚点是引用环内 invalid 邮件，不属于任何会话（-> 409）。"""


class RetentionFreezeNotFound(Exception):
    """解除冻结：不存在以该邮件为锚点的冻结（-> 404）。"""


# 游标载荷版本号；编码结构调整时可据此区分。
_CURSOR_VERSION = 1
# 合法的 base64url（无填充）字符集，先于解码拦截伪造字符。
_CURSOR_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_message(row: sqlite3.Row) -> ThreadMessage:
    return ThreadMessage(
        message_id=row["message_id"],
        in_reply_to=row["in_reply_to"],
        references=tuple(json.loads(row["references"])),
        sent_at=row["sent_at"],
        subject=row["subject"],
    )


def _climb_root(conn: sqlite3.Connection, message_id: str) -> str:
    """沿 parent_id 向上爬到根（新邮件均为有效邮件，链不会断）。"""

    cur = message_id
    seen: set[str] = set()
    while True:
        row = conn.execute(
            "SELECT parent_id FROM messages WHERE message_id = ?", (cur,)
        ).fetchone()
        pid = row["parent_id"] if row else None
        if pid is None or pid in seen:
            return cur
        seen.add(cur)
        cur = pid


def _load_subtree(conn: sqlite3.Connection, root_id: str) -> tuple[dict[str, ThreadMessage], dict[str, Optional[str]]]:
    """递归取出某根下全部有效节点（parent_id 在树内）。"""

    rows = conn.execute(
        """
        WITH RECURSIVE tree(message_id, in_reply_to, "references", sent_at, subject, parent_id, depth) AS (
            SELECT message_id, in_reply_to, "references", sent_at, subject, parent_id, 0
            FROM messages WHERE message_id = ?
            UNION ALL
            SELECT m.message_id, m.in_reply_to, m."references", m.sent_at, m.subject, m.parent_id, t.depth + 1
            FROM messages m JOIN tree t ON m.parent_id = t.message_id
            WHERE m.is_invalid = 0
        )
        SELECT message_id, in_reply_to, "references", sent_at, subject, parent_id
        FROM tree ORDER BY depth
        """,
        (root_id,),
    ).fetchall()

    messages: dict[str, ThreadMessage] = {}
    parent_of: dict[str, Optional[str]] = {}
    for row in rows:
        mid = row["message_id"]
        messages[mid] = _row_to_message(row)
        parent_of[mid] = row["parent_id"]
    return messages, parent_of


def _build_payload(
    batch_id: str,
    submitted_at: str,
    normalized_messages: list,
    resolution: threading.BatchResolution,
    archived_duplicates: dict[str, int],
    threads: list[dict],
) -> dict:
    """组装对外响应（同时也是持久化快照）。"""

    duplicates = []
    # 已在历史批次归档的重复 ID
    for mid in sorted(archived_duplicates):
        duplicates.append({
            "message_id": mid,
            "archived": True,
            "first_submitted_seq": None,
            "occurrences": [archived_duplicates[mid]],
        })
    # 本批内的重复 ID
    for dup in resolution.duplicates:
        duplicates.append({
            "message_id": dup.message_id,
            "archived": False,
            "first_submitted_seq": dup.first_submitted_seq,
            "occurrences": dup.occurrences,
        })
    duplicates.sort(key=lambda d: (d["message_id"], 0 if d["archived"] else 1))

    missing_references = [
        {"message_id": m.message_id, "missing": m.missing}
        for m in resolution.missing_refs
    ]

    by_id = {m.message_id: m for m in normalized_messages}
    cycles_out: list[dict] = []
    invalid_messages: list[dict] = []
    for idx, cyc in enumerate(resolution.cycles, start=1):
        cycles_out.append({"index": idx, "members": list(cyc), "invalid_messages": list(cyc)})
        for mid in cyc:
            m = by_id[mid]
            invalid_messages.append({
                "message_id": mid,
                "in_reply_to": m.in_reply_to,
                "references": list(m.references),
                "sent_at": m.sent_at.isoformat(),
                "subject": m.subject,
                "reason": "reference_cycle",
                "cycle_index": idx,
            })
    invalid_messages.sort(key=lambda im: im["message_id"])

    total_count = len(normalized_messages)
    unique_count = len(resolution.first_seen)
    invalid_count = len(resolution.invalid_ids)

    return {
        "batch_id": batch_id,
        "submitted_at": submitted_at,
        "totals": {
            "submitted": total_count,
            "unique_new": unique_count,
            "archived_duplicates": len(archived_duplicates),
            "invalid": invalid_count,
            "missing_reference_messages": len(resolution.missing_refs),
            "cycles": len(resolution.cycles),
            "trees": len(threads),
        },
        "threads": threads,
        "duplicates": duplicates,
        "missing_references": missing_references,
        "cycles": cycles_out,
        "invalid_messages": invalid_messages,
    }


def submit_batch(payload: BatchIn) -> dict:
    """归并、落库、返回完整结果。同库写操作串行化。"""

    normalized = [m.normalized() for m in payload.messages]

    with database.write_lock():
        conn = database.connect()
        try:
            existing_ids = {
                r["message_id"]
                for r in conn.execute("SELECT message_id FROM messages")
            }

            # 已归档 ID 整组剔出，作为“重复 ID（历史归档）”诊断
            archived_duplicates: dict[str, int] = {}
            fresh: list = []
            for seq, m in enumerate(normalized, start=1):
                if m.message_id in existing_ids:
                    archived_duplicates.setdefault(m.message_id, seq)
                else:
                    fresh.append(m)
            thread_messages = [
                ThreadMessage(
                    message_id=m.message_id,
                    in_reply_to=m.in_reply_to,
                    references=tuple(m.references),
                    sent_at=m.sent_at.isoformat(),
                    subject=m.subject,
                )
                for m in fresh
            ]
            resolution = threading.resolve_batch(
                thread_messages, existing_ids=existing_ids
            )

            batch_id = uuid.uuid4().hex
            submitted_at = _utc_now_iso()

            # 1) 批次元数据（messages/batch_* 均以外键指向它）
            conn.execute(
                """
                INSERT INTO batches
                    (batch_id, submitted_at, total_count, unique_count, invalid_count, tree_count)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    submitted_at,
                    len(normalized),
                    len(resolution.first_seen),
                    len(resolution.invalid_ids),
                    0,  # 树数量在构建后回填
                ),
            )

            # 2) 写入新邮件（含 invalid；invalid 父指针置 NULL，绝不部分挂接）
            inserted_ids: set[str] = set()
            for seq, m in enumerate(normalized, start=1):
                if m.message_id in inserted_ids or m.message_id in archived_duplicates:
                    continue  # 本批重复的后续出现、历史归档重复，均不重复落库
                inserted_ids.add(m.message_id)
                is_invalid = m.message_id in resolution.invalid_ids
                parent_id = None if is_invalid else resolution.parent_of.get(m.message_id)
                conn.execute(
                    """
                    INSERT INTO messages
                        (message_id, in_reply_to, "references", sent_at, subject,
                         parent_id, is_invalid, batch_id, submitted_seq, archived_at,
                         retain_until)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        m.message_id,
                        m.in_reply_to,
                        json.dumps(m.references, ensure_ascii=False),
                        m.sent_at.isoformat(),
                        m.subject,
                        parent_id,
                        1 if is_invalid else 0,
                        batch_id,
                        seq,
                        submitted_at,
                        m.retain_until.isoformat() if m.retain_until else None,
                    ),
                )

            # 3) 本批所有有效新邮件向上爬到根，取整棵树（含历史祖先）
            new_valid_ids = [
                mid for mid in resolution.first_seen if mid not in resolution.invalid_ids
            ]
            root_ids = {_climb_root(conn, mid) for mid in new_valid_ids}

            all_messages: dict[str, ThreadMessage] = {}
            all_parents: dict[str, Optional[str]] = {}
            for root_id in root_ids:
                msgs, parents = _load_subtree(conn, root_id)
                all_messages.update(msgs)
                all_parents.update(parents)

            threads = threading.build_forest(all_messages, all_parents)

            result_payload = _build_payload(
                batch_id,
                submitted_at,
                normalized,
                resolution,
                archived_duplicates,
                threads,
            )

            # 4) 回填树数量 + 诊断明细
            conn.execute(
                "UPDATE batches SET tree_count = ? WHERE batch_id = ?",
                (len(threads), batch_id),
            )
            for dup in result_payload["duplicates"]:
                conn.execute(
                    "INSERT INTO batch_duplicates (batch_id, message_id, archived, first_submitted_seq, occurrences) VALUES (?, ?, ?, ?, ?)",
                    (
                        batch_id,
                        dup["message_id"],
                        1 if dup["archived"] else 0,
                        dup["first_submitted_seq"],
                        json.dumps(dup["occurrences"]),
                    ),
                )
            for mr in result_payload["missing_references"]:
                conn.execute(
                    "INSERT INTO batch_missing_refs (batch_id, message_id, missing) VALUES (?, ?, ?)",
                    (batch_id, mr["message_id"], json.dumps(mr["missing"], ensure_ascii=False)),
                )
            for cyc in result_payload["cycles"]:
                conn.execute(
                    "INSERT INTO batch_cycles (batch_id, idx, members) VALUES (?, ?, ?)",
                    (batch_id, cyc["index"], json.dumps(cyc["members"], ensure_ascii=False)),
                )

            # 5) 结果快照
            conn.execute(
                "INSERT INTO batch_results (batch_id, result_json) VALUES (?, ?)",
                (batch_id, json.dumps(result_payload, ensure_ascii=False)),
            )

            conn.commit()
            return result_payload
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _climb_root_inmem(
    message_id: str, parent_of: dict[str, Optional[str]]
) -> str:
    """在内存父指针图上向上爬到根（遇到不存在或环时回退为当前节点）。"""

    cur = message_id
    seen: set[str] = set()
    while True:
        pid = parent_of.get(cur)
        if pid is None or pid in seen:
            return cur
        seen.add(cur)
        cur = pid


def relink_messages(payload: RelinkIn) -> dict:
    """迟到邮件归档后，批量修复此前断链的有效邮件。

    - 每封邮件仅在当前 parent_id 为空（断链自成根）且非环内 invalid 时处理；
    - 候选父仍按原规则：先取现存且有效的 in_reply_to，再从 references
      右向左回退；无法命中则保持不变；
    - 全部候选关系合并到既有父指针图上统一检测环，任一变更成环则整次
      抛 RelinkCycleConflict，绝不写库；
    - 无环时在单事务中更新 parent_id，返回每封邮件的新旧 parent/root。
    """

    with database.write_lock():
        conn = database.connect()
        try:
            rows = conn.execute(
                """
                SELECT message_id, in_reply_to, "references", parent_id, is_invalid
                FROM messages
                WHERE message_id IN (%s)
                """ % ",".join("?" * len(payload.message_ids)),
                payload.message_ids,
            ).fetchall() if payload.message_ids else []

            by_id = {r["message_id"]: r for r in rows}
            missing = [mid for mid in payload.message_ids if mid not in by_id]
            if missing:
                raise RelinkNotFound(missing)

            # 既有整图：所有已归档有效邮件的当前父指针
            base_parents: dict[str, Optional[str]] = {
                r["message_id"]: r["parent_id"]
                for r in conn.execute(
                    "SELECT message_id, parent_id FROM messages WHERE is_invalid = 0"
                )
            }
            # 环内 invalid 邮件不是“有效”节点，不可作为候选父
            invalid_ids = {
                r["message_id"]
                for r in conn.execute(
                    "SELECT message_id FROM messages WHERE is_invalid = 1"
                )
            }

            # 逐封计算候选；仅有效且当前为断链根的邮件参与
            candidates: dict[str, Optional[str]] = {}
            for mid in payload.message_ids:
                row = by_id[mid]
                if row["is_invalid"] or row["parent_id"] is not None:
                    continue
                msg = ThreadMessage(
                    message_id=mid,
                    in_reply_to=row["in_reply_to"],
                    references=tuple(json.loads(row["references"])),
                    sent_at="",
                    subject="",
                )
                # 已知集合 = 有效邮件全集；环成员(invalid) 不作为父
                candidates[mid] = threading.pick_candidate(
                    msg, set(base_parents), invalid_ids=invalid_ids
                )

            # 合并全部候选关系后统一检测环
            merged_parents = dict(base_parents)
            for mid, new_pid in candidates.items():
                merged_parents[mid] = new_pid

            cycles = threading.find_cycles(merged_parents)
            if cycles:
                raise RelinkCycleConflict(cycles)

            results: list[dict] = []
            relinked = 0
            updates: list[tuple[Optional[str], str]] = []
            for mid in payload.message_ids:
                row = by_id[mid]
                old_pid = row["parent_id"]
                new_pid = candidates.get(mid, old_pid)
                changed = new_pid != old_pid
                if changed:
                    relinked += 1
                    updates.append((new_pid, mid))
                old_root = (
                    mid
                    if old_pid is None or row["is_invalid"]
                    else _climb_root_inmem(mid, base_parents)
                )
                new_root = (
                    mid
                    if new_pid is None or row["is_invalid"]
                    else _climb_root_inmem(mid, merged_parents)
                )
                results.append({
                    "message_id": mid,
                    "old_parent_id": old_pid,
                    "new_parent_id": new_pid,
                    "old_root_id": old_root,
                    "new_root_id": new_root,
                })

            # 单事务提交全部父指针变更
            for new_pid, mid in updates:
                conn.execute(
                    "UPDATE messages SET parent_id = ? WHERE message_id = ?",
                    (new_pid, mid),
                )
            conn.commit()
            return {"relinked": relinked, "results": results}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def list_batches(limit: int, offset: int) -> dict:
    conn = database.connect()
    try:
        total = conn.execute("SELECT COUNT(*) AS c FROM batches").fetchone()["c"]
        rows = conn.execute(
            "SELECT * FROM batches ORDER BY submitted_at DESC, batch_id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        items = [
            {
                "batch_id": r["batch_id"],
                "submitted_at": r["submitted_at"],
                "total_count": r["total_count"],
                "unique_count": r["unique_count"],
                "invalid_count": r["invalid_count"],
                "tree_count": r["tree_count"],
            }
            for r in rows
        ]
        return {"total": total, "limit": limit, "offset": offset, "batches": items}
    finally:
        conn.close()


def get_batch(batch_id: str) -> Optional[dict]:
    conn = database.connect()
    try:
        row = conn.execute(
            "SELECT result_json FROM batch_results WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        return json.loads(row["result_json"]) if row else None
    finally:
        conn.close()


def get_message(message_id: str) -> Optional[dict]:
    conn = database.connect()
    try:
        row = conn.execute(
            'SELECT message_id, in_reply_to, "references", sent_at, subject, parent_id, is_invalid, batch_id, retain_until FROM messages WHERE message_id = ?',
            (message_id,),
        ).fetchone()
        if row is None:
            return None
        frozen = conn.execute(
            "SELECT 1 FROM retention_freeze_members WHERE message_id = ? LIMIT 1",
            (message_id,),
        ).fetchone()
        return {
            "message_id": row["message_id"],
            "in_reply_to": row["in_reply_to"],
            "references": json.loads(row["references"]),
            "sent_at": row["sent_at"],
            "subject": row["subject"],
            "parent_id": row["parent_id"],
            "is_invalid": bool(row["is_invalid"]),
            "batch_id": row["batch_id"],
            "retain_until": row["retain_until"],
            "frozen": frozen is not None,
        }
    finally:
        conn.close()


def get_thread(message_id: str) -> Optional[dict]:
    """返回某邮件所在会话的完整树。环邮件不挂接，返回 409。"""

    conn = database.connect()
    try:
        row = conn.execute(
            "SELECT is_invalid FROM messages WHERE message_id = ?", (message_id,)
        ).fetchone()
        if row is None:
            return None
        if row["is_invalid"]:
            return {"invalid": True, "message_id": message_id}

        root_id = _climb_root(conn, message_id)
        msgs, parents = _load_subtree(conn, root_id)
        forest = threading.build_forest(msgs, parents)
        return {"root_id": root_id, "tree": forest[0] if forest else None}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /threads/{message_id}/page：稳定游标分页
# ---------------------------------------------------------------------------


def _tree_revision(
    messages: dict[str, ThreadMessage],
    parent_of: dict[str, Optional[str]],
) -> str:
    """根据整棵树的 message_id、parent_id、sent_at 生成 revision。

    节点按 message_id 排序后逐行摘要，补链改变父指针、新邮件并入树等任何
    结构变化都会使摘要改变，从而让旧游标失效。
    """

    digest = hashlib.sha256()
    for mid in sorted(messages):
        msg = messages[mid]
        line = "\x1f".join(
            [mid, parent_of.get(mid) or "", msg.sent_at]
        )
        digest.update(line.encode("utf-8"))
        digest.update(b"\x1e")
    return digest.hexdigest()


def _encode_cursor(root_id: str, offset: int, revision: str) -> str:
    """把根 ID、下一偏移、树 revision 封装为不透明 base64url 游标。"""

    payload = {
        "v": _CURSOR_VERSION,
        "root_id": root_id,
        "offset": offset,
        "revision": revision,
    }
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _decode_cursor(cursor: str) -> tuple[str, int, str]:
    """解析游标；任何格式 / 字段问题统一抛 CursorMalformed。"""

    try:
        if not _CURSOR_TOKEN_RE.fullmatch(cursor):
            raise ValueError("bad charset")
        padding = "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(cursor + padding)
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise CursorMalformed("cursor is malformed") from exc

    if (
        not isinstance(payload, dict)
        or set(payload) != {"v", "root_id", "offset", "revision"}
        or payload["v"] != _CURSOR_VERSION
        or not isinstance(payload["root_id"], str)
        or not isinstance(payload["offset"], int)
        or isinstance(payload["offset"], bool)
        or payload["offset"] < 0
        or not isinstance(payload["revision"], str)
    ):
        raise CursorMalformed("cursor is malformed")
    return payload["root_id"], payload["offset"], payload["revision"]


def get_thread_page(
    message_id: str,
    limit: int,
    cursor: Optional[str],
) -> dict:
    """先序扁平分页查询某邮件所属会话。

    首次请求不带 cursor：从偏移 0 起；后续请求携带上次响应的 next_cursor。
    游标封装根 ID、下一偏移与整树 revision：树在补链中发生变化返回
    CursorStale（409 cursor_stale）；游标与路径所属会话不符返回
    CursorThreadMismatch（422）。
    """

    start_offset = 0
    cursor_root: Optional[str] = None
    cursor_revision: Optional[str] = None
    if cursor:
        cursor_root, start_offset, cursor_revision = _decode_cursor(cursor)

    conn = database.connect()
    try:
        row = conn.execute(
            "SELECT is_invalid FROM messages WHERE message_id = ?", (message_id,)
        ).fetchone()
        if row is None:
            return None
        if row["is_invalid"]:
            return {"invalid": True, "message_id": message_id}

        root_id = _climb_root(conn, message_id)
        msgs, parents = _load_subtree(conn, root_id)
        items = threading.build_flat_tree(msgs, parents)
        revision = _tree_revision(msgs, parents)

        if cursor_root is not None:
            if cursor_root != root_id:
                # 游标根是当前树内节点（补链并树后下沉为子节点）：树已变 -> 409
                if cursor_root in msgs:
                    raise CursorStale("cursor revision is stale; restart from the first page")
                # 游标根已被保留期清理删除（同一谱系被压缩）：树已变 -> 409
                ghost = conn.execute(
                    "SELECT 1 FROM messages WHERE message_id = ?", (cursor_root,)
                ).fetchone()
                if ghost is None:
                    raise CursorStale(
                        "cursor root has been removed by retention cleanup; "
                        "restart from the first page"
                    )
                # 游标根不属于路径所属会话 -> 422
                raise CursorThreadMismatch(
                    "cursor does not belong to the thread of the path message_id"
                )
            if cursor_revision != revision:
                raise CursorStale("cursor revision is stale; restart from the first page")
            if start_offset > len(items):
                raise CursorStale("cursor offset is beyond the current thread")

        page = items[start_offset:start_offset + limit]
        next_offset = start_offset + len(page)
        next_cursor = (
            _encode_cursor(root_id, next_offset, revision)
            if next_offset < len(items)
            else None
        )

        return {
            "root_id": root_id,
            "items": page,
            "next_cursor": next_cursor,
            "limit": limit,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 保留期：会话冻结 / 解除冻结 / 批量清理
# ---------------------------------------------------------------------------


def _load_protected_ids(conn: sqlite3.Connection) -> set[str]:
    """当前被任一冻结覆盖的全部成员 ID（多冻结取并集）。"""

    return {
        r["message_id"]
        for r in conn.execute("SELECT DISTINCT message_id FROM retention_freeze_members")
    }


def freeze_thread(payload: FreezeIn) -> dict:
    """冻结锚点邮件当前所在的整棵会话。

    冻结只保护“冻结时刻”的成员快照：之后新并入该会话的邮件不受保护，
    需再次以该锚点冻结来刷新快照。锚点不存在 -> 404；锚点为环内
    invalid 邮件（不属于任何会话树）-> 409。
    """

    anchor_id = payload.message_id
    with database.write_lock():
        conn = database.connect()
        try:
            row = conn.execute(
                "SELECT is_invalid FROM messages WHERE message_id = ?",
                (anchor_id,),
            ).fetchone()
            if row is None:
                raise RetentionAnchorNotFound("message not found")
            if row["is_invalid"]:
                raise RetentionAnchorInvalid(
                    "message belongs to a reference cycle and is not attached to any thread"
                )

            root_id = _climb_root(conn, anchor_id)
            msgs, parents = _load_subtree(conn, root_id)
            # 成员顺序与会话查询/分页一致：稳定排序的先序
            members = [item["message_id"] for item in threading.build_flat_tree(msgs, parents)]

            existing = conn.execute(
                "SELECT frozen_at FROM retention_freezes WHERE anchor_id = ?",
                (anchor_id,),
            ).fetchone()
            frozen_at = _utc_now_iso()
            if existing is None:
                created = True
                conn.execute(
                    "INSERT INTO retention_freezes (anchor_id, frozen_at) VALUES (?, ?)",
                    (anchor_id, frozen_at),
                )
            else:
                created = False
                conn.execute(
                    "UPDATE retention_freezes SET frozen_at = ? WHERE anchor_id = ?",
                    (frozen_at, anchor_id),
                )
                # 刷新成员快照：清掉旧成员后按当前会话重写
                conn.execute(
                    "DELETE FROM retention_freeze_members WHERE anchor_id = ?",
                    (anchor_id,),
                )
            conn.executemany(
                "INSERT INTO retention_freeze_members (anchor_id, message_id) VALUES (?, ?)",
                [(anchor_id, mid) for mid in members],
            )
            conn.commit()
            return {
                "anchor_id": anchor_id,
                "root_id": root_id,
                "frozen_at": frozen_at,
                "created": created,
                "members": members,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def unfreeze_thread(payload: UnfreezeIn) -> dict:
    """解除以锚点邮件建立的冻结，仅释放该冻结独占的保护。"""

    anchor_id = payload.message_id
    with database.write_lock():
        conn = database.connect()
        try:
            row = conn.execute(
                "SELECT frozen_at FROM retention_freezes WHERE anchor_id = ?",
                (anchor_id,),
            ).fetchone()
            if row is None:
                raise RetentionFreezeNotFound("freeze not found for anchor message")
            frozen_at = row["frozen_at"]

            members = [
                r["message_id"]
                for r in conn.execute(
                    "SELECT message_id FROM retention_freeze_members WHERE anchor_id = ? ORDER BY message_id",
                    (anchor_id,),
                )
            ]

            # 冻结记录里的根 ID：沿当前父指针上溯（锚点永不会被清理删除）
            root_id = _climb_root(conn, anchor_id)

            conn.execute(
                "DELETE FROM retention_freezes WHERE anchor_id = ?",
                (anchor_id,),
            )
            # ON DELETE CASCADE 在独立连接级 PRAGMA 下已开启；这里不依赖它，显式删成员
            conn.execute(
                "DELETE FROM retention_freeze_members WHERE anchor_id = ?",
                (anchor_id,),
            )

            placeholders = ",".join("?" * len(members))
            still_protected = (
                [
                    r["message_id"]
                    for r in conn.execute(
                        f"SELECT DISTINCT message_id FROM retention_freeze_members WHERE message_id IN ({placeholders}) ORDER BY message_id",
                        members,
                    )
                ]
                if members
                else []
            )
            released_members = [mid for mid in members if mid not in set(still_protected)]

            conn.commit()
            return {
                "anchor_id": anchor_id,
                "root_id": root_id,
                "frozen_at": frozen_at,
                "released_members": released_members,
                "still_protected": still_protected,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _build_cleanup_plan(
    conn: sqlite3.Connection, cutoff_iso: str
) -> tuple[RetentionCleanupPlan, dict[str, Optional[str]]]:
    """读取当前有效邮件森林并计算到期清理计划（dry_run 与实跑共用）。"""

    rows = conn.execute(
        "SELECT message_id, parent_id FROM messages WHERE is_invalid = 0"
    ).fetchall()
    parent_of: dict[str, Optional[str]] = {r["message_id"]: r["parent_id"] for r in rows}

    expired_rows = conn.execute(
        "SELECT message_id FROM messages WHERE is_invalid = 0 AND retain_until IS NOT NULL AND retain_until <= ?",
        (cutoff_iso,),
    ).fetchall()
    expired_ids = {r["message_id"] for r in expired_rows}
    protected_ids = _load_protected_ids(conn)

    plan = threading.plan_retention_cleanup(parent_of, expired_ids, protected_ids)
    return plan, parent_of


def cleanup_messages(payload: CleanupIn) -> dict:
    """批量清理截止时刻前已到期、且未被冻结保护的有效邮件。

    筛出删除集后先在内存父指针图上做整林压缩（最近存活祖先重挂 /
    提升为新根），dry_run 返回同一算法结论但不写库；实际清理在单事务
    内完成全部 DELETE 与 parent_id UPDATE。
    """

    cutoff_dt = payload.normalized_cutoff() or datetime.now(timezone.utc)
    cutoff_iso = cutoff_dt.isoformat()

    if payload.dry_run:
        conn = database.connect()
        try:
            plan, _ = _build_cleanup_plan(conn, cutoff_iso)
        finally:
            conn.close()
        return _cleanup_result(payload.dry_run, cutoff_dt, plan)

    with database.write_lock():
        conn = database.connect()
        try:
            plan, _ = _build_cleanup_plan(conn, cutoff_iso)

            # 单事务：先重挂存活后代，再删除到期节点
            for impact in plan.impacts:
                conn.execute(
                    "UPDATE messages SET parent_id = ? WHERE message_id = ?",
                    (impact.new_parent_id, impact.message_id),
                )
            if plan.deleted_ids:
                placeholders = ",".join("?" * len(plan.deleted_ids))
                conn.execute(
                    f"DELETE FROM messages WHERE message_id IN ({placeholders})",
                    plan.deleted_ids,
                )
            conn.commit()
            return _cleanup_result(payload.dry_run, cutoff_dt, plan)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _cleanup_result(
    dry_run: bool, cutoff_dt: datetime, plan: RetentionCleanupPlan
) -> dict:
    return {
        "dry_run": dry_run,
        "cutoff": cutoff_dt.isoformat(),
        "deleted_ids": plan.deleted_ids,
        "affected": [
            {
                "message_id": impact.message_id,
                "old_parent_id": impact.old_parent_id,
                "new_parent_id": impact.new_parent_id,
                "old_root_id": impact.old_root_id,
                "new_root_id": impact.new_root_id,
            }
            for impact in plan.impacts
        ],
        "deleted_count": len(plan.deleted_ids),
        "affected_count": len(plan.impacts),
        "expired_count": len(plan.expired_ids),
        "protected_count": len(plan.protected_expired_ids),
    }
