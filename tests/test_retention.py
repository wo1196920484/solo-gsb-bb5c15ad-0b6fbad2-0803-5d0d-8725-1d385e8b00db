"""保留期清理与会话冻结的端到端测试。"""

from __future__ import annotations

from datetime import datetime, timezone


def _m(mid, irt=None, refs=None, sent="2026-09-18T10:00:00+00:00", subject="hello",
       retain=None):
    msg = {
        "message_id": mid,
        "in_reply_to": irt,
        "references": refs or [],
        "sent_at": sent,
        "subject": subject,
    }
    if retain is not None:
        msg["retain_until"] = retain
    return msg


def _seed_chain(client, retains=None):
    """归档 A -> B -> C -> D 链（跨两个批次），返回各邮件 ID 列表。"""

    retains = retains or {}
    client.post("/batches", json={"messages": [
        _m("A", sent="2026-09-18T08:00:00+00:00", retain=retains.get("A")),
        _m("B", irt="A", refs=["A"], sent="2026-09-18T09:00:00+00:00",
           retain=retains.get("B")),
    ]})
    client.post("/batches", json={"messages": [
        _m("C", irt="B", refs=["A", "B"], sent="2026-09-18T10:00:00+00:00",
           retain=retains.get("C")),
        _m("D", irt="C", refs=["A", "B", "C"], sent="2026-09-18T11:00:00+00:00",
           retain=retains.get("D")),
    ]})
    return ["A", "B", "C", "D"]


CUTOFF = "2026-10-01T00:00:00+00:00"
PAST = "2026-09-30T23:59:59+00:00"
FUTURE = "2026-10-01T00:00:01+00:00"


# ---------------------------------------------------------------------------
# retain_until 字段
# ---------------------------------------------------------------------------


def test_retain_until_stored_normalized_and_optional(client):
    client.post("/batches", json={"messages": [
        _m("A", retain="2026-10-02T08:00:00+08:00"),   # 带偏移 -> 归一化 UTC
        _m("B", sent="2026-09-18T09:00:00+00:00",
           retain="2026-10-03T00:00:00"),               # 无时区 -> 按 UTC
        _m("C", sent="2026-09-18T10:00:00+00:00"),      # 未填写 -> null
    ]})

    a = client.get("/messages/A").json()
    b = client.get("/messages/B").json()
    c = client.get("/messages/C").json()
    assert datetime.fromisoformat(a["retain_until"]) == datetime(
        2026, 10, 2, 0, 0, tzinfo=timezone.utc
    )
    assert datetime.fromisoformat(b["retain_until"]) == datetime(
        2026, 10, 3, 0, 0, tzinfo=timezone.utc
    )
    assert c["retain_until"] is None


# ---------------------------------------------------------------------------
# 冻结 / 解除冻结
# ---------------------------------------------------------------------------


def test_freeze_unfreeze_roundtrip_and_listing(client):
    _seed_chain(client)

    r = client.post("/freezes", json={"message_id": "C"})
    assert r.status_code == 200
    body = r.json()
    assert body["message_id"] == "C"
    assert body["root_id"] == "A"
    assert body["protected_count"] == 4
    assert body["already_frozen"] is False
    assert body["frozen_at"]

    # 幂等：重复冻结同一邮件
    again = client.post("/freezes", json={"message_id": "C"}).json()
    assert again["already_frozen"] is True
    assert again["root_id"] == "A"
    assert again["protected_count"] == 4

    listed = client.get("/freezes").json()
    assert listed["total"] == 1
    assert listed["freezes"][0]["message_id"] == "C"
    assert listed["freezes"][0]["root_id"] == "A"

    r = client.delete("/freezes/C")
    assert r.status_code == 200
    assert r.json() == {"message_id": "C", "unfrozen": True}
    assert client.get("/freezes").json()["total"] == 0

    # 再解除 -> 404
    assert client.delete("/freezes/C").status_code == 404


def test_freeze_unknown_and_invalid_message(client):
    assert client.post("/freezes", json={"message_id": "ghost"}).status_code == 404

    client.post("/batches", json={"messages": [
        _m("x", irt="y", refs=["y"], sent="2026-09-18T12:00:00+00:00"),
        _m("y", irt="x", refs=["x"], sent="2026-09-18T13:00:00+00:00"),
    ]})
    assert client.post("/freezes", json={"message_id": "x"}).status_code == 409
    assert client.get("/freezes").json()["total"] == 0


def test_freeze_blank_id_rejected(client):
    assert client.post("/freezes", json={"message_id": "   "}).status_code == 422
    assert client.post("/freezes", json={}).status_code == 422


def test_unfreeze_unknown_id_returns_404(client):
    assert client.delete("/freezes/ghost").status_code == 404


# ---------------------------------------------------------------------------
# 清理：删除与重挂
# ---------------------------------------------------------------------------


def test_cleanup_deletes_middle_node_and_reparents_descendants(client):
    _seed_chain(client, retains={"B": PAST})

    r = client.post("/cleanup", json={"before": CUTOFF})
    assert r.status_code == 200
    body = r.json()
    assert body["dry_run"] is False
    assert body["deleted_count"] == 1
    assert body["deleted_message_ids"] == ["B"]
    # C 重挂到最近的保留祖先 A；D 父节点未变，不在 affected 中
    assert body["affected"] == [{
        "message_id": "C",
        "old_parent_id": "B",
        "new_parent_id": "A",
        "old_root_id": "A",
        "new_root_id": "A",
    }]

    # 会话查询立即反映压缩后的结构
    assert client.get("/messages/B").status_code == 404
    assert client.get("/messages/C").json()["parent_id"] == "A"
    tree = client.get("/threads/D").json()
    assert tree["root_id"] == "A"
    assert [c["message_id"] for c in tree["tree"]["children"]] == ["C"]
    assert [g["message_id"] for g in tree["tree"]["children"][0]["children"]] == ["D"]


def test_cleanup_root_deletion_promotes_child_to_new_root(client):
    _seed_chain(client, retains={"A": PAST})

    body = client.post("/cleanup", json={"before": CUTOFF}).json()
    assert body["deleted_message_ids"] == ["A"]
    assert body["affected"] == [{
        "message_id": "B",
        "old_parent_id": "A",
        "new_parent_id": None,
        "old_root_id": "A",
        "new_root_id": "B",
    }]
    assert client.get("/threads/D").json()["root_id"] == "B"


def test_cleanup_multiple_deletions_cascade_reattach(client):
    _seed_chain(client, retains={"A": PAST, "C": PAST})

    body = client.post("/cleanup", json={"before": CUTOFF}).json()
    assert body["deleted_message_ids"] == ["A", "C"]
    assert body["affected"] == [
        {"message_id": "B", "old_parent_id": "A", "new_parent_id": None,
         "old_root_id": "A", "new_root_id": "B"},
        {"message_id": "D", "old_parent_id": "C", "new_parent_id": "B",
         "old_root_id": "A", "new_root_id": "B"},
    ]
    tree = client.get("/threads/D").json()
    assert tree["root_id"] == "B"
    assert [c["message_id"] for c in tree["tree"]["children"]] == ["D"]


def test_cleanup_deletes_whole_expired_tree(client):
    _seed_chain(client, retains={"A": PAST, "B": PAST, "C": PAST, "D": PAST})

    body = client.post("/cleanup", json={"before": CUTOFF}).json()
    assert body["deleted_message_ids"] == ["A", "B", "C", "D"]
    assert body["affected"] == []
    assert client.get("/messages/A").status_code == 404
    assert client.get("/threads/D").status_code == 404


def test_cleanup_nothing_expired(client):
    _seed_chain(client, retains={"B": FUTURE})  # 未到期
    _ = client.post("/batches", json={"messages": [_m("E", sent="2026-09-18T12:00:00+00:00")]})

    body = client.post("/cleanup", json={"before": CUTOFF}).json()
    assert body["deleted_count"] == 0
    assert body["deleted_message_ids"] == []
    assert body["affected"] == []
    assert client.get("/messages/E").status_code == 200


def test_cleanup_cutoff_boundary_is_inclusive(client):
    client.post("/batches", json={"messages": [
        _m("M1", retain=CUTOFF),       # 恰好在截止时刻到期 -> 删除
        _m("M2", sent="2026-09-18T09:00:00+00:00", retain=FUTURE),  # 晚于截止 -> 保留
    ]})
    body = client.post("/cleanup", json={"before": CUTOFF}).json()
    assert body["deleted_message_ids"] == ["M1"]
    assert client.get("/messages/M2").status_code == 200


def test_cleanup_ignores_invalid_messages(client):
    client.post("/batches", json={"messages": [
        _m("x", irt="y", refs=["y"], sent="2026-09-18T12:00:00+00:00", retain=PAST),
        _m("y", irt="x", refs=["x"], sent="2026-09-18T13:00:00+00:00", retain=PAST),
    ]})
    body = client.post("/cleanup", json={"before": CUTOFF}).json()
    assert body["deleted_count"] == 0
    # 环内 invalid 邮件即使到期也不清理
    assert client.get("/messages/x").status_code == 200
    assert client.get("/messages/y").status_code == 200


def test_cleanup_requires_before(client):
    assert client.post("/cleanup", json={}).status_code == 422
    assert client.post("/cleanup", json={"before": "not-a-time"}).status_code == 422


# ---------------------------------------------------------------------------
# dry_run：同一算法，不写库
# ---------------------------------------------------------------------------


def test_cleanup_dry_run_uses_same_algorithm_but_writes_nothing(client):
    _seed_chain(client, retains={"A": PAST, "C": PAST})

    dry = client.post("/cleanup", json={"before": CUTOFF, "dry_run": True}).json()
    assert dry["dry_run"] is True
    assert dry["deleted_message_ids"] == ["A", "C"]
    assert [a["message_id"] for a in dry["affected"]] == ["B", "D"]

    # 试算后库中结构保持原样
    for mid in ("A", "B", "C", "D"):
        assert client.get(f"/messages/{mid}").status_code == 200
    assert client.get("/messages/B").json()["parent_id"] == "A"
    assert client.get("/threads/D").json()["root_id"] == "A"

    # 实际执行与试算结果一致（除 dry_run 标志）
    real = client.post("/cleanup", json={"before": CUTOFF}).json()
    assert real["dry_run"] is False
    assert {k: v for k, v in real.items() if k != "dry_run"} == \
           {k: v for k, v in dry.items() if k != "dry_run"}
    assert client.get("/messages/A").status_code == 404
    assert client.get("/threads/D").json()["root_id"] == "B"


# ---------------------------------------------------------------------------
# 冻结保护
# ---------------------------------------------------------------------------


def test_frozen_thread_is_protected_until_unfrozen(client):
    # 树 1：F -> G（均到期）；树 2：H（到期）-> I（保留）
    client.post("/batches", json={"messages": [
        _m("F", sent="2026-09-18T08:00:00+00:00", retain=PAST),
        _m("G", irt="F", refs=["F"], sent="2026-09-18T09:00:00+00:00", retain=PAST),
        _m("H", sent="2026-09-18T10:00:00+00:00", retain=PAST),
        _m("I", irt="H", refs=["H"], sent="2026-09-18T11:00:00+00:00"),
    ]})
    client.post("/freezes", json={"message_id": "G"})

    body = client.post("/cleanup", json={"before": CUTOFF}).json()
    # 冻结保护整棵会话 F/G；仅 H 被清理，I 重挂为新根
    assert body["protected_count"] == 2
    assert body["deleted_message_ids"] == ["H"]
    assert body["affected"] == [
        {"message_id": "I", "old_parent_id": "H", "new_parent_id": None,
         "old_root_id": "H", "new_root_id": "I"},
    ]
    assert client.get("/messages/F").status_code == 200
    assert client.get("/messages/G").status_code == 200

    # 解冻后同一清理即可删除
    client.delete("/freezes/G")
    body = client.post("/cleanup", json={"before": CUTOFF}).json()
    assert body["protected_count"] == 0
    assert body["deleted_message_ids"] == ["F", "G"]


def test_freeze_protects_conversation_dynamically(client):
    # 冻结后新并入会话的邮件同样受保护
    client.post("/batches", json={"messages": [
        _m("A", sent="2026-09-18T08:00:00+00:00"),
    ]})
    client.post("/freezes", json={"message_id": "A"})
    client.post("/batches", json={"messages": [
        _m("E", irt="A", refs=["A"], sent="2026-09-18T09:00:00+00:00", retain=PAST),
    ]})

    body = client.post("/cleanup", json={"before": CUTOFF}).json()
    assert body["protected_count"] == 2
    assert body["deleted_count"] == 0
    assert client.get("/messages/E").status_code == 200


# ---------------------------------------------------------------------------
# 快照不变 / 查询与分页立即反映压缩结构
# ---------------------------------------------------------------------------


def test_cleanup_keeps_batch_snapshots_unchanged(client):
    _seed_chain(client, retains={"B": PAST})
    listed = client.get("/batches").json()
    snaps_before = [
        client.get(f"/batches/{b['batch_id']}").json() for b in listed["batches"]
    ]

    client.post("/cleanup", json={"before": CUTOFF})

    snaps_after = [
        client.get(f"/batches/{b['batch_id']}").json() for b in listed["batches"]
    ]
    assert snaps_after == snaps_before
    # 快照里 B 依然存在（归档时原样）
    assert any(
        n["message_id"] == "B"
        for snap in snaps_after
        for n in _flatten(snap["threads"])
    )


def _flatten(nodes):
    out = []
    for n in nodes:
        out.append(n)
        out.extend(_flatten(n["children"]))
    return out


def test_cleanup_thread_page_reflects_compression_and_stales_cursor(client):
    _seed_chain(client, retains={"B": PAST})

    first = client.get("/threads/A/page", params={"limit": 2}).json()
    assert [i["message_id"] for i in first["items"]] == ["A", "B"]
    cursor = first["next_cursor"]
    assert cursor

    client.post("/cleanup", json={"before": CUTOFF})

    # 旧游标因树结构变化失效
    stale = client.get("/threads/A/page", params={"limit": 2, "cursor": cursor})
    assert stale.status_code == 409
    assert stale.json()["detail"]["error"] == "cursor_stale"

    # 重新分页立即得到压缩后的先序结构（C 重挂到 A 下，depth 收敛）
    fresh = client.get("/threads/A/page", params={"limit": 200}).json()
    assert [(i["message_id"], i["depth"], i["parent_id"]) for i in fresh["items"]] == [
        ("A", 0, None),
        ("C", 1, "A"),
        ("D", 2, "C"),
    ]
    assert fresh["next_cursor"] is None


# ---------------------------------------------------------------------------
# 既有数据库的轻量迁移
# ---------------------------------------------------------------------------


def test_init_db_migrates_old_schema(tmp_path, monkeypatch):
    """旧版库（无 retain_until 列、无 freezes 表）升级后列/表/索引齐备且数据完好。"""

    import importlib
    import sqlite3

    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.executescript("""
    CREATE TABLE batches (batch_id TEXT PRIMARY KEY, submitted_at TEXT NOT NULL,
        total_count INTEGER NOT NULL, unique_count INTEGER NOT NULL,
        invalid_count INTEGER NOT NULL, tree_count INTEGER NOT NULL);
    CREATE TABLE messages (
        message_id TEXT PRIMARY KEY, in_reply_to TEXT,
        "references" TEXT NOT NULL DEFAULT '[]', sent_at TEXT NOT NULL,
        subject TEXT NOT NULL, parent_id TEXT, is_invalid INTEGER NOT NULL DEFAULT 0,
        batch_id TEXT NOT NULL, submitted_seq INTEGER NOT NULL, archived_at TEXT NOT NULL,
        FOREIGN KEY (batch_id) REFERENCES batches(batch_id));
    INSERT INTO batches VALUES ('b1', '2026-09-18T00:00:00+00:00', 1, 1, 0, 1);
    INSERT INTO messages VALUES ('A', NULL, '[]', '2026-09-18T08:00:00+00:00',
        's', NULL, 0, 'b1', 1, '2026-09-18T00:00:00+00:00');
    """)
    conn.commit()
    conn.close()

    monkeypatch.setenv("DATABASE_PATH", str(db))
    from app import config, database
    importlib.reload(config)
    importlib.reload(database)
    database.init_db()
    database.init_db()  # 幂等

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(messages)")}
        assert "retain_until" in cols
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert "freezes" in tables
        idx = {r[1] for r in conn.execute("PRAGMA index_list('messages')")}
        assert "idx_messages_retain" in idx
        row = conn.execute(
            "SELECT message_id, retain_until FROM messages WHERE message_id='A'"
        ).fetchone()
        assert row["message_id"] == "A" and row["retain_until"] is None
    finally:
        conn.close()
