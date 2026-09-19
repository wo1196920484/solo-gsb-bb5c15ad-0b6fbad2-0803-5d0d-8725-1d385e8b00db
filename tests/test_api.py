"""端到端 API 测试：提交、跨批归并、诊断、查询、断链补链。"""

from __future__ import annotations

import importlib


def _m(mid, irt=None, refs=None, sent="2026-09-18T10:00:00+00:00", subject="hello"):
    return {
        "message_id": mid,
        "in_reply_to": irt,
        "references": refs or [],
        "sent_at": sent,
        "subject": subject,
    }


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_submit_and_get_batch_snapshot(client):
    payload = {"messages": [
        _m("A", sent="2026-09-18T09:00:00+00:00"),
        _m("B", irt="A", refs=["A"], sent="2026-09-18T10:00:00+00:00"),
    ]}
    r = client.post("/batches", json=payload)
    assert r.status_code == 201
    body = r.json()
    assert body["totals"]["submitted"] == 2
    assert body["totals"]["trees"] == 1

    # 快照查询结果一致
    again = client.get(f"/batches/{body['batch_id']}").json()
    assert again == body

    listed = client.get("/batches").json()
    assert listed["total"] == 1
    assert listed["batches"][0]["batch_id"] == body["batch_id"]


def test_missing_references_and_broken_chain(client):
    r = client.post("/batches", json={"messages": [
        _m("X", irt="nope", refs=["g1", "g2"]),
    ]}).json()

    assert r["threads"][0]["message_id"] == "X"
    assert r["threads"][0]["parent_id"] is None
    assert r["missing_references"] == [
        {"message_id": "X", "missing": ["g1", "g2", "nope"]}
    ]


def test_full_cycle_invalid_and_no_partial_attach(client):
    body = client.post("/batches", json={"messages": [
        _m("a", irt="c", refs=["c"], sent="2026-09-18T01:00:00+00:00"),
        _m("b", irt="a", refs=["c", "a"], sent="2026-09-18T02:00:00+00:00"),
        _m("c", irt="b", refs=["c", "a", "b"], sent="2026-09-18T03:00:00+00:00"),
    ]}).json()

    assert body["threads"] == []
    assert body["totals"]["invalid"] == 3
    assert body["totals"]["cycles"] == 1
    cyc = body["cycles"][0]
    assert set(cyc["members"]) == {"a", "b", "c"}
    assert sorted(i["message_id"] for i in body["invalid_messages"]) == ["a", "b", "c"]

    # 环邮件查询：消息存在但线程接口返回 409
    got = client.get("/messages/a").json()
    assert got["is_invalid"] is True
    assert got["parent_id"] is None
    resp = client.get("/threads/a")
    assert resp.status_code == 409


def test_duplicate_ids_in_batch(client):
    body = client.post("/batches", json={"messages": [
        _m("A"),
        _m("D", irt="A", sent="2026-09-18T11:00:00+00:00"),
        _m("D", irt=None, refs=[], sent="2026-09-18T12:00:00+00:00"),
    ]}).json()

    dups = body["duplicates"]
    assert len(dups) == 1
    assert dups[0]["message_id"] == "D"
    assert dups[0]["archived"] is False
    assert dups[0]["occurrences"] == [2, 3]
    assert body["totals"]["submitted"] == 3
    assert body["totals"]["unique_new"] == 2


def test_cross_batch_threading_and_archived_duplicate(client):
    b1 = client.post("/batches", json={"messages": [_m("A")]}).json()
    b2 = client.post("/batches", json={"messages": [
        _m("B", irt="A", sent="2026-09-18T11:00:00+00:00"),
        _m("A"),  # 已归档重复
    ]}).json()

    # B 跨批挂到历史 A 之下
    tree = b2["threads"][0]
    assert tree["message_id"] == "A"
    assert [c["message_id"] for c in tree["children"]] == ["B"]

    assert b2["duplicates"] == [{
        "message_id": "A",
        "archived": True,
        "first_submitted_seq": None,
        "occurrences": [2],
    }]
    assert b2["totals"]["archived_duplicates"] == 1

    # 线程查询返回完整跨批树
    t = client.get("/threads/B").json()
    assert t["root_id"] == "A"
    assert t["tree"]["children"][0]["message_id"] == "B"


def test_cycle_tail_falls_back_to_valid_reference(client):
    body = client.post("/batches", json={"messages": [
        _m("R", sent="2026-09-18T00:00:00+00:00"),
        _m("x", irt="y", refs=["y"], sent="2026-09-18T01:00:00+00:00"),
        _m("y", irt="x", refs=["x"], sent="2026-09-18T02:00:00+00:00"),
        _m("z", irt="y", refs=["R"], sent="2026-09-18T03:00:00+00:00"),
    ]}).json()

    # x/y 整环 invalid；z 回退挂到 R
    assert body["totals"]["invalid"] == 2
    tree = body["threads"][0]
    assert tree["message_id"] == "R"
    assert [c["message_id"] for c in tree["children"]] == ["z"]


def test_message_not_found(client):
    assert client.get("/messages/zzz").status_code == 404
    assert client.get("/threads/zzz").status_code == 404
    assert client.get("/batches/zzz").status_code == 404


def test_empty_batch_rejected(client):
    assert client.post("/batches", json={"messages": []}).status_code == 422


# ---------------------------------------------------------------------------
# POST /relinks：迟到邮件归档后批量修复断链
# ---------------------------------------------------------------------------


def test_relink_uses_late_in_reply_to_and_updates_threads_and_roots(client):
    b1 = client.post("/batches", json={"messages": [
        _m("A", sent="2026-09-18T08:00:00+00:00"),
        _m("D", irt="late", refs=["ghost"], sent="2026-09-18T09:00:00+00:00"),
    ]}).json()
    assert client.get("/messages/D").json()["parent_id"] is None

    # 迟到邮件归档（其本身挂到 A 下）
    client.post("/batches", json={"messages": [
        _m("late", irt="A", refs=["A"], sent="2026-09-18T07:00:00+00:00"),
    ]})

    r = client.post("/relinks", json={"message_ids": ["D"]})
    assert r.status_code == 200
    body = r.json()
    assert body["relinked"] == 1
    assert body["results"] == [{
        "message_id": "D",
        "old_parent_id": None,
        "new_parent_id": "late",
        "old_root_id": "D",
        "new_root_id": "A",
    }]

    # GET /threads 随即反映新树：A -> late -> D
    tree = client.get("/threads/D").json()
    assert tree["root_id"] == "A"
    assert tree["tree"]["message_id"] == "A"
    assert [c["message_id"] for c in tree["tree"]["children"]] == ["late"]
    assert [g["message_id"] for g in tree["tree"]["children"][0]["children"]] == ["D"]
    assert client.get("/messages/D").json()["parent_id"] == "late"

    # 既有批次结果快照保持不变：快照里 D 仍是断链根
    snap = client.get(f"/batches/{b1['batch_id']}").json()
    snap_d = [t for t in snap["threads"] if t["message_id"] == "D"][0]
    assert snap_d["parent_id"] is None
    assert snap == b1


def test_relink_is_idempotent(client):
    client.post("/batches", json={"messages": [
        _m("A", sent="2026-09-18T08:00:00+00:00"),
        _m("D", irt="A-missing", refs=["late-A"], sent="2026-09-18T09:00:00+00:00"),
    ]})
    client.post("/batches", json={"messages": [
        _m("late-A", sent="2026-09-18T07:00:00+00:00"),
    ]})
    first = client.post("/relinks", json={"message_ids": ["D"]}).json()
    assert first["relinked"] == 1

    again = client.post("/relinks", json={"message_ids": ["D"]}).json()
    assert again["relinked"] == 0
    assert again["results"][0] == {
        "message_id": "D",
        "old_parent_id": "late-A",
        "new_parent_id": "late-A",
        "old_root_id": "late-A",
        "new_root_id": "late-A",
    }


def test_relink_falls_back_through_references_right_to_left(client):
    # 归档时所有引用都不存在 -> 断链根；迟到两个候选后补链，取 references 最右已知项
    client.post("/batches", json={"messages": [
        _m("F", irt="still-missing", refs=["g1", "late1", "late2"],
           sent="2026-09-18T10:00:00+00:00"),
    ]})
    client.post("/batches", json={"messages": [
        _m("late1", sent="2026-09-18T06:00:00+00:00"),
        _m("late2", irt="late1", refs=["late1"], sent="2026-09-18T07:00:00+00:00"),
    ]})
    body = client.post("/relinks", json={"message_ids": ["F"]}).json()
    item = body["results"][0]
    assert item["old_parent_id"] is None
    assert item["new_parent_id"] == "late2"
    assert item["old_root_id"] == "F"
    assert item["new_root_id"] == "late1"


def test_relink_no_candidate_stays_unchanged(client):
    client.post("/batches", json={"messages": [
        _m("G", irt="nope", refs=["n1", "n2"], sent="2026-09-18T10:00:00+00:00"),
    ]})
    body = client.post("/relinks", json={"message_ids": ["G"]}).json()
    assert body["relinked"] == 0
    assert body["results"][0] == {
        "message_id": "G",
        "old_parent_id": None,
        "new_parent_id": None,
        "old_root_id": "G",
        "new_root_id": "G",
    }
    assert client.get("/messages/G").json()["parent_id"] is None


def test_relink_skips_already_attached_and_invalid(client):
    client.post("/batches", json={"messages": [
        _m("R", sent="2026-09-18T00:00:00+00:00"),
        _m("H", irt="R", refs=["R"], sent="2026-09-18T11:00:00+00:00"),
        _m("cyc1", irt="cyc2", refs=["cyc2"], sent="2026-09-18T12:00:00+00:00"),
        _m("cyc2", irt="cyc1", refs=["cyc1"], sent="2026-09-18T13:00:00+00:00"),
    ]})
    body = client.post("/relinks", json={"message_ids": ["H", "cyc1"]}).json()
    assert body["relinked"] == 0
    assert body["results"] == [
        {"message_id": "H", "old_parent_id": "R", "new_parent_id": "R",
         "old_root_id": "R", "new_root_id": "R"},
        {"message_id": "cyc1", "old_parent_id": None, "new_parent_id": None,
         "old_root_id": "cyc1", "new_root_id": "cyc1"},
    ]


def test_relink_unknown_id_returns_404_and_writes_nothing(client):
    client.post("/batches", json={"messages": [_m("A")]})
    r = client.post("/relinks", json={"message_ids": ["A", "ghost-id"]})
    assert r.status_code == 404
    assert r.json()["detail"]["message_ids"] == ["ghost-id"]
    # A 即便本可命中也整批不处理
    assert client.get("/messages/A").json()["parent_id"] is None


def test_relink_empty_and_blank_ids_rejected(client):
    assert client.post("/relinks", json={"message_ids": []}).status_code == 422
    assert client.post("/relinks", json={"message_ids": ["   "]}).status_code == 422


def test_relink_dedupes_ids_and_preserves_order(client):
    client.post("/batches", json={"messages": [
        _m("A", sent="2026-09-18T08:00:00+00:00"),
        _m("G", irt="nope", refs=["n1"], sent="2026-09-18T09:00:00+00:00"),
    ]})
    body = client.post("/relinks", json={"message_ids": ["G", "A", "G", " A "]}).json()
    assert [r["message_id"] for r in body["results"]] == ["G", "A"]


def _open_db():
    """按当前 DATABASE_PATH 直连测试库（绕过服务连接）。"""
    import sqlite3

    from app import config

    importlib.reload(config)
    conn = sqlite3.connect(config.settings.database_path)
    conn.row_factory = sqlite3.Row
    return conn


def test_relink_merged_cycle_returns_409_and_writes_nothing(client):
    # 真实归档不会自然产生两个互指的有效根；直接构造该初始状态，
    # 验证候选关系合并后统一检测环、整次 409 且不写库。
    client.post("/batches", json={"messages": [_m("p", sent="2026-09-18T14:00:00+00:00")]})
    client.post("/batches", json={"messages": [_m("q", sent="2026-09-18T15:00:00+00:00")]})
    conn = _open_db()
    try:
        conn.execute("UPDATE messages SET in_reply_to='q', \"references\"='[\"q\"]' WHERE message_id='p'")
        conn.execute("UPDATE messages SET in_reply_to='p', \"references\"='[\"p\"]' WHERE message_id='q'")
        conn.commit()
    finally:
        conn.close()

    r = client.post("/relinks", json={"message_ids": ["p", "q"]})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["cycles"] == [["p", "q"]]

    conn = _open_db()
    try:
        rows = {row["message_id"]: row["parent_id"]
                for row in conn.execute("SELECT message_id, parent_id FROM messages")}
    finally:
        conn.close()
    assert rows == {"p": None, "q": None}

    # 只修一封可以成功；再修第二封时与既有边合并又成环 -> 409 且不写库
    assert client.post("/relinks", json={"message_ids": ["p"]}).json()["results"][0]["new_parent_id"] == "q"
    r2 = client.post("/relinks", json={"message_ids": ["q"]})
    assert r2.status_code == 409
    conn = _open_db()
    try:
        row = conn.execute("SELECT parent_id FROM messages WHERE message_id='q'").fetchone()
    finally:
        conn.close()
    assert row["parent_id"] is None


def test_relink_invalid_cycle_members_are_not_candidate_parents(client):
    # ca/cb 同批成环 invalid；M 只引用环成员、N 的 references 指向迟到邮件：
    # 环剔除后 M/N 都断链为根。补链时环成员同样不得作为候选父。
    client.post("/batches", json={"messages": [
        _m("ca", irt="cb", refs=["cb"], sent="2026-09-18T18:00:00+00:00"),
        _m("cb", irt="ca", refs=["ca"], sent="2026-09-18T18:10:00+00:00"),
        _m("M", irt="ca", refs=["ca"], sent="2026-09-18T18:20:00+00:00"),
        _m("N", irt="ca", refs=["lateV"], sent="2026-09-18T18:30:00+00:00"),
    ]})
    assert client.get("/messages/M").json()["parent_id"] is None
    assert client.get("/messages/N").json()["parent_id"] is None

    body = client.post("/relinks", json={"message_ids": ["M"]}).json()
    assert body["relinked"] == 0
    assert body["results"][0] == {
        "message_id": "M", "old_parent_id": None, "new_parent_id": None,
        "old_root_id": "M", "new_root_id": "M",
    }

    # lateV 迟到归档后，N 越过环成员回退到 references 中的有效项
    client.post("/batches", json={"messages": [
        _m("lateV", sent="2026-09-18T05:00:00+00:00"),
    ]})
    item = client.post("/relinks", json={"message_ids": ["N"]}).json()["results"][0]
    assert item["old_parent_id"] is None
    assert item["new_parent_id"] == "lateV"
    assert item["new_root_id"] == "lateV"


def test_relink_self_candidate_is_a_cycle(client):
    client.post("/batches", json={"messages": [_m("zz", sent="2026-09-18T16:00:00+00:00")]})
    conn = _open_db()
    try:
        conn.execute("UPDATE messages SET in_reply_to='zz', \"references\"='[]' WHERE message_id='zz'")
        conn.commit()
    finally:
        conn.close()
    r = client.post("/relinks", json={"message_ids": ["zz"]})
    assert r.status_code == 409
    assert r.json()["detail"]["cycles"] == [["zz"]]


# ---------------------------------------------------------------------------
# GET /threads/{message_id}/page：稳定游标分页
# ---------------------------------------------------------------------------


def _seed_paged_thread(client):
    """归档 R -> B -> C、R -> D 的会话；先序应为 R(0) B(1) C(2) D(1)。"""

    client.post("/batches", json={"messages": [
        _m("R", sent="2026-09-18T08:00:00+00:00"),
        _m("B", irt="R", refs=["R"], sent="2026-09-18T09:00:00+00:00"),
        _m("C", irt="B", refs=["R", "B"], sent="2026-09-18T10:00:00+00:00"),
        _m("D", irt="R", refs=["R"], sent="2026-09-18T11:00:00+00:00"),
    ]})


def test_thread_page_first_page_is_flat_preorder_with_depth(client):
    _seed_paged_thread(client)
    r = client.get("/threads/R/page", params={"limit": 2})
    assert r.status_code == 200
    body = r.json()
    assert body["root_id"] == "R"
    assert body["limit"] == 2
    assert [(i["message_id"], i["depth"]) for i in body["items"]] == [
        ("R", 0),
        ("B", 1),
    ]
    assert body["next_cursor"]

    item = body["items"][1]
    assert set(item) == {
        "message_id", "parent_id", "in_reply_to", "references",
        "sent_at", "subject", "depth",
    }
    assert item["parent_id"] == "R"
    assert item["references"] == ["R"]


def test_thread_page_walks_all_pages_without_gaps_or_dupes(client):
    _seed_paged_thread(client)
    seen: list[tuple[str, int]] = []
    cursor = None
    pages = 0
    while True:
        params = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        body = client.get("/threads/D/page", params=params).json()
        pages += 1
        seen.extend((i["message_id"], i["depth"]) for i in body["items"])
        cursor = body["next_cursor"]
        if not cursor:
            break
        assert pages <= 3
    assert seen == [("R", 0), ("B", 1), ("C", 2), ("D", 1)]
    assert pages == 2  # 4 条 / 每页 2 条

    # 最后一页 next_cursor 为 null；重复使用倒数游标得到稳定的同一尾页
    last = client.get("/threads/R/page", params={"limit": 2}).json()
    mid_cursor = last["next_cursor"]
    tail1 = client.get("/threads/R/page", params={"limit": 2, "cursor": mid_cursor}).json()
    tail2 = client.get("/threads/R/page", params={"limit": 2, "cursor": mid_cursor}).json()
    assert tail1["next_cursor"] is None
    assert [i["message_id"] for i in tail1["items"]] == ["C", "D"]
    assert tail1 == tail2


def test_thread_page_large_limit_covers_everything_in_one_page(client):
    _seed_paged_thread(client)
    body = client.get("/threads/R/page", params={"limit": 200}).json()
    assert [i["message_id"] for i in body["items"]] == ["R", "B", "C", "D"]
    assert body["next_cursor"] is None


def test_thread_page_order_matches_nested_thread_preorder(client):
    _seed_paged_thread(client)

    def preorder(nodes):
        out = []
        for n in nodes:
            out.append(n["message_id"])
            out.extend(preorder(n["children"]))
        return out

    nested = client.get("/threads/C").json()["tree"]
    flat = client.get("/threads/R/page", params={"limit": 200}).json()["items"]
    assert [i["message_id"] for i in flat] == preorder([nested])


def test_thread_page_limit_bounds_validated(client):
    _seed_paged_thread(client)
    assert client.get("/threads/R/page", params={"limit": 0}).status_code == 422
    assert client.get("/threads/R/page", params={"limit": 201}).status_code == 422
    bad = client.get("/threads/R/page", params={"limit": "two"})
    assert bad.status_code == 422


def test_thread_page_unknown_message_returns_404(client):
    assert client.get("/threads/nope/page").status_code == 404


def test_thread_page_cycle_message_returns_409(client):
    client.post("/batches", json={"messages": [
        _m("x", irt="y", refs=["y"], sent="2026-09-18T12:00:00+00:00"),
        _m("y", irt="x", refs=["x"], sent="2026-09-18T13:00:00+00:00"),
    ]})
    r = client.get("/threads/x/page")
    assert r.status_code == 409


def test_thread_page_malformed_cursor_returns_422(client):
    _seed_paged_thread(client)
    import base64
    import json as _json

    def tok(obj) -> str:
        return base64.urlsafe_b64encode(
            _json.dumps(obj).encode()
        ).rstrip(b"=").decode()

    for bad in [
        "not a cursor!!!",          # 非法字符
        tok({"v": 1}),               # 缺字段
        tok({"v": 2, "root_id": "R", "offset": 0, "revision": "x"}),  # 版本不符
        tok({"v": 1, "root_id": "R", "offset": -1, "revision": "x"}),  # 偏移为负
        tok({"v": 1, "root_id": 9, "offset": 0, "revision": "x"}),    # 类型错误
        tok(["not", "an", "object"]),
        "eyJhIjoxfQ",                # 任意 base64url 但非本游标结构
    ]:
        r = client.get("/threads/R/page", params={"limit": 2, "cursor": bad})
        assert r.status_code == 422, bad


def test_thread_page_cursor_for_other_thread_returns_422(client):
    _seed_paged_thread(client)
    # 另一棵互不相干的树 P
    client.post("/batches", json={"messages": [
        _m("P", sent="2026-09-19T08:00:00+00:00"),
        _m("Q", irt="P", refs=["P"], sent="2026-09-19T09:00:00+00:00"),
    ]})
    foreign = client.get("/threads/P/page", params={"limit": 1}).json()["next_cursor"]
    r = client.get("/threads/R/page", params={"limit": 2, "cursor": foreign})
    assert r.status_code == 422
    # 反向同样不允许
    home = client.get("/threads/R/page", params={"limit": 1}).json()["next_cursor"]
    assert client.get("/threads/P/page", params={"cursor": home}).status_code == 422


def test_thread_page_stale_cursor_after_relink_returns_409(client):
    # D 先归档、其父 late 尚未到达 -> D 断链为根；E 正常挂在 D 下
    client.post("/batches", json={"messages": [
        _m("D", irt="late", refs=["late"], sent="2026-09-18T09:00:00+00:00"),
        _m("E", irt="D", refs=["D"], sent="2026-09-18T10:00:00+00:00"),
    ]})
    # 迟到邮件随后归档，自成独立根，L2 挂在 late 下
    client.post("/batches", json={"messages": [
        _m("late", sent="2026-09-18T07:00:00+00:00"),
        _m("L2", irt="late", refs=["late"], sent="2026-09-18T07:30:00+00:00"),
    ]})

    # 游标 1：late 树（根不变，但补链后会并入 D）-> revision 不符
    cur_same_root = client.get(
        "/threads/late/page", params={"limit": 1}
    ).json()["next_cursor"]
    # 游标 2：D 当时自成根的树（补链后 D 下沉为 late 的子节点）
    cur_old_root = client.get(
        "/threads/D/page", params={"limit": 1}
    ).json()["next_cursor"]
    assert cur_same_root and cur_old_root

    # 补链：D 挂到 late 下，两棵树合并、结构改变
    r = client.post("/relinks", json={"message_ids": ["D"]})
    assert r.status_code == 200
    assert r.json()["results"][0]["new_parent_id"] == "late"

    stale1 = client.get(
        "/threads/late/page", params={"limit": 1, "cursor": cur_same_root}
    )
    assert stale1.status_code == 409
    assert stale1.json()["detail"]["error"] == "cursor_stale"

    stale2 = client.get(
        "/threads/D/page", params={"limit": 1, "cursor": cur_old_root}
    )
    assert stale2.status_code == 409
    assert stale2.json()["detail"]["error"] == "cursor_stale"

    # 不带游标从头开始即正常：late 树下按先序 late L2 D E
    fresh = client.get("/threads/D/page", params={"limit": 200}).json()
    assert [(i["message_id"], i["depth"]) for i in fresh["items"]] == [
        ("late", 0),
        ("L2", 1),
        ("D", 1),
        ("E", 2),
    ]
    assert fresh["next_cursor"] is None


def test_thread_page_stale_cursor_when_thread_grows_via_new_batch(client):
    _seed_paged_thread(client)
    cur = client.get("/threads/R/page", params={"limit": 2}).json()["next_cursor"]

    # 新批次又往同一棵树挂一封邮件，revision 必须改变
    client.post("/batches", json={"messages": [
        _m("E", irt="D", refs=["R", "D"], sent="2026-09-18T12:00:00+00:00"),
    ]})
    r = client.get("/threads/R/page", params={"limit": 2, "cursor": cur})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "cursor_stale"


def test_thread_page_endpoint_does_not_change_existing_thread_endpoint(client):
    _seed_paged_thread(client)
    tree = client.get("/threads/R").json()
    assert tree["root_id"] == "R"
    assert tree["tree"]["message_id"] == "R"
    assert [c["message_id"] for c in tree["tree"]["children"]] == ["B", "D"]
    assert tree["tree"]["children"][0]["children"][0]["message_id"] == "C"
