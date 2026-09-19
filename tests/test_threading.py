"""threading.resolve_batch 纯逻辑测试。"""

from __future__ import annotations

from app.threading import ThreadMessage, build_flat_tree, build_forest, resolve_batch


def msg(mid, irt=None, refs=(), sent="2026-09-18T00:00:00+00:00", subj="s"):
    return ThreadMessage(mid, irt, tuple(refs), sent, subj)

def ids(forest):
    def walk(nodes):
        out = []
        for n in nodes:
            out.append(n["message_id"])
            out.extend(walk(n["children"]))
        return out

    return walk(forest)


def test_basic_thread_in_reply_to_preferred_over_last_reference():
    # in_reply_to 有效 => 优先采用，即使 references 最后一个已知项是别人
    r = resolve_batch([
        msg("A"),
        msg("B"),
        msg("C", irt="A", refs=["B", "A"]),
    ])
    assert r.parent_of["C"] == "A"
    forest = build_forest(r.first_seen, r.parent_of)
    assert ids(forest) == ["A", "C", "B"]  # 根/子均按 (sent_at, id) 全序排


def test_fallback_to_last_known_reference():
    # in_reply_to 缺失；references 含未知项时取最后一个已知项
    r = resolve_batch([
        msg("A"),
        msg("C", irt=None, refs=["ghost", "A", "ghost2"]),
    ])
    assert r.parent_of["C"] == "A"
    miss = {m.message_id: m.missing for m in r.missing_refs}
    assert miss["C"] == ["ghost", "ghost2"]


def test_broken_chain_becomes_root():
    r = resolve_batch([
        msg("X", irt="missing-parent", refs=["g1", "g2"]),
    ])
    assert r.parent_of["X"] is None
    assert r.roots == ["X"]
    miss = {m.message_id: m.missing for m in r.missing_refs}
    assert miss["X"] == ["g1", "g2", "missing-parent"]
    forest = build_forest(r.first_seen, r.parent_of)
    assert forest[0]["message_id"] == "X"
    assert forest[0]["parent_id"] is None


def test_self_reference_is_one_cycle_invalid():
    r = resolve_batch([msg("S", irt="S")])
    assert r.invalid_ids == {"S"}
    assert r.cycles == [["S"]]
    assert "S" not in r.parent_of  # 不下沉为根


def test_whole_cycle_marked_invalid_no_partial_attachment():
    # A->B, B->C, C->A：三环整环 invalid，任何一个都不许挂接
    r = resolve_batch([
        msg("A", irt="C"),
        msg("B", irt="A"),
        msg("C", irt="B"),
    ])
    assert r.invalid_ids == {"A", "B", "C"}
    assert len(r.cycles) == 1
    assert set(r.cycles[0]) == {"A", "B", "C"}
    assert r.parent_of == {}
    assert r.roots == []


def test_cycle_tail_reattaches_to_valid_reference_after_invalidation():
    # D 的 in_reply_to 指向环上 B，但其 references 中还含有有效根 R：
    # 环 A<->B 整体 invalid；D 必须从 references 回退到 R，而不是成为孤儿/根
    r = resolve_batch([
        msg("R"),
        msg("A", irt="B"),
        msg("B", irt="A"),
        msg("D", irt="B", refs=["R"]),
    ])
    assert r.invalid_ids == {"A", "B"}
    assert r.parent_of["D"] == "R"
    assert r.parent_of["R"] is None
    forest = build_forest(r.first_seen, r.parent_of)
    assert ids(forest) == ["R", "D"]


def test_cycle_tail_without_valid_ref_becomes_root_not_partial_attach():
    # E 仅引用环成员；环被剔除后 E 断链自成根（而非错误挂到环上）
    r = resolve_batch([
        msg("A", irt="B"),
        msg("B", irt="A"),
        msg("E", irt=None, refs=["A"]),
    ])
    assert r.parent_of["E"] is None
    assert r.roots == ["E"]


def test_duplicates_in_batch_keep_first_only():
    r = resolve_batch([
        msg("A"),
        msg("X", irt="A", sent="2026-09-18T01:00:00+00:00"),
        msg("X", irt=None, refs=["A"], sent="2026-09-18T02:00:00+00:00"),
        msg("X", irt=None, refs=[], sent="2026-09-18T03:00:00+00:00"),
    ])
    assert len(r.first_seen) == 2
    assert len(r.duplicates) == 1
    d = r.duplicates[0]
    assert d.message_id == "X"
    assert d.first_submitted_seq == 2
    assert d.occurrences == [2, 3, 4]
    # 采用首次出现：in_reply_to=A
    assert r.parent_of["X"] == "A"


def test_existing_ids_serve_as_parents_and_never_cycle():
    r = resolve_batch(
        [msg("new", irt="old", refs=["phantom"])],
        existing_ids=["old"],
    )
    assert r.parent_of["new"] == "old"
    assert r.invalid_ids == set()
    miss = {m.message_id: m.missing for m in r.missing_refs}
    assert miss["new"] == ["phantom"]


def test_stable_sort_by_sent_at_then_id():
    r = resolve_batch([
        msg("later", irt="root", sent="2026-09-18T05:00:00+00:00"),
        msg("early", irt="root", sent="2026-09-18T01:00:00+00:00"),
        msg("root", sent="2026-09-18T00:00:00+00:00"),
    ])
    forest = build_forest(r.first_seen, r.parent_of)
    root = forest[0]
    assert root["message_id"] == "root"
    assert [c["message_id"] for c in root["children"]] == ["early", "later"]


def test_deterministic_cycle_rotation():
    # 环统一从环内最小 ID 开始展示
    r = resolve_batch([
        msg("m3", irt="m1"),
        msg("m1", irt="m2"),
        msg("m2", irt="m3"),
    ])
    assert r.cycles[0][0] == "m1"


def test_two_disjoint_cycles_both_invalid():
    r = resolve_batch([
        msg("a1", irt="a2"), msg("a2", irt="a1"),
        msg("b1", irt="b2"), msg("b2", irt="b1"),
        msg("root"),
    ])
    assert r.invalid_ids == {"a1", "a2", "b1", "b2"}
    assert len(r.cycles) == 2
    assert sorted(c[0] for c in r.cycles) == ["a1", "b1"]
    assert r.roots == ["root"]


def test_deep_thread_forest_order():
    r = resolve_batch([
        msg("root1", sent="2026-09-18T00:00:00+00:00"),
        msg("root2", sent="2026-09-18T01:00:00+00:00"),
        msg("c1", irt="root1", sent="2026-09-18T03:00:00+00:00"),
        msg("c2", irt="root1", sent="2026-09-18T02:00:00+00:00"),
        msg("gc", irt="c1", sent="2026-09-18T04:00:00+00:00"),
    ])
    forest = build_forest(r.first_seen, r.parent_of)
    assert [t["message_id"] for t in forest] == ["root1", "root2"]
    assert [c["message_id"] for c in forest[0]["children"]] == ["c2", "c1"]
    assert forest[0]["children"][1]["children"][0]["message_id"] == "gc"


def test_cycle_does_not_poison_unrelated_tree():
    r = resolve_batch([
        msg("ok", irt="goodroot"),
        msg("goodroot"),
        msg("p", irt="q"), msg("q", irt="p"),
    ], existing_ids=[])
    assert r.parent_of["ok"] == "goodroot"
    assert r.parent_of["goodroot"] is None
    forest = build_forest(r.first_seen, r.parent_of)
    assert ids(forest) == ["goodroot", "ok"]


def test_build_flat_tree_matches_forest_preorder_and_depth():
    r = resolve_batch([
        msg("root1", sent="2026-09-18T00:00:00+00:00"),
        msg("root2", sent="2026-09-18T01:00:00+00:00"),
        msg("c1", irt="root1", sent="2026-09-18T03:00:00+00:00"),
        msg("c2", irt="root1", sent="2026-09-18T02:00:00+00:00"),
        msg("gc", irt="c1", sent="2026-09-18T04:00:00+00:00"),
    ])
    flat = build_flat_tree(r.first_seen, r.parent_of)

    # 与嵌套树先序完全一致
    assert [f["message_id"] for f in flat] == ids(build_forest(r.first_seen, r.parent_of))
    # depth：根为 0，逐层递增
    assert [(f["message_id"], f["depth"], f["parent_id"]) for f in flat] == [
        ("root1", 0, None),
        ("c2", 1, "root1"),
        ("c1", 1, "root1"),
        ("gc", 2, "c1"),
        ("root2", 0, None),
    ]
    # 扁平节点不含 children，其余树节点字段保留
    assert "children" not in flat[0]
    assert flat[0]["subject"] == "s"
    assert flat[0]["references"] == []


def test_build_flat_tree_empty_when_no_messages():
    assert build_flat_tree({}, {}) == []
