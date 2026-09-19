"""threading.resolve_batch 纯逻辑测试。"""

from __future__ import annotations

from app.threading import (
    ThreadMessage,
    build_flat_tree,
    build_forest,
    plan_retention_cleanup,
    resolve_batch,
)


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


# ---------------------------------------------------------------------------
# plan_retention_cleanup：到期删除 + 会话压缩（纯逻辑）
# ---------------------------------------------------------------------------


def _impact_map(plan):
    return {i.message_id: i for i in plan.impacts}


def test_cleanup_deletes_expired_leaves_and_keeps_unexpired():
    # R -> A(到期) -> C(未到期)；D(无 retain_until) 挂在 R 下
    parent_of = {"R": None, "A": "R", "C": "A", "D": "R"}
    plan = plan_retention_cleanup(parent_of, expired_ids={"A"}, protected_ids=set())

    assert plan.deleted_ids == ["A"]
    assert plan.expired_ids == frozenset({"A"})
    assert plan.protected_expired_ids == frozenset()
    impacts = _impact_map(plan)
    # C 重挂到最近存活祖先 R；根不变
    c = impacts["C"]
    assert (c.old_parent_id, c.new_parent_id, c.old_root_id, c.new_root_id) == (
        "A", "R", "R", "R",
    )
    # R/D 未受影响
    assert "R" not in impacts and "D" not in impacts


def test_cleanup_deleting_middle_of_chain_skips_to_nearest_surviving_ancestor():
    # R -> B(到期) -> C(到期) -> D -> E
    parent_of = {"R": None, "B": "R", "C": "B", "D": "C", "E": "D"}
    plan = plan_retention_cleanup(
        parent_of, expired_ids={"B", "C"}, protected_ids=set()
    )
    assert plan.deleted_ids == ["B", "C"]
    impacts = _impact_map(plan)
    # D 跳过 C、B，重挂到 R；E 父是仍存活的 D，故 E 根不变、父不变
    d = impacts["D"]
    assert d.new_parent_id == "R"
    assert d.old_root_id == "R" and d.new_root_id == "R"
    assert "E" not in impacts


def test_cleanup_deleting_root_promotes_subtree_to_new_roots():
    # R(到期) -> B -> C；整棵树删除根后，B 成为新根；C 父不变但根变化
    parent_of = {"R": None, "B": "R", "C": "B"}
    plan = plan_retention_cleanup(parent_of, expired_ids={"R"}, protected_ids=set())
    assert plan.deleted_ids == ["R"]
    impacts = _impact_map(plan)
    b = impacts["B"]
    assert (b.old_parent_id, b.new_parent_id, b.old_root_id, b.new_root_id) == (
        "R", None, "R", "B",
    )
    c = impacts["C"]
    assert (c.old_parent_id, c.new_parent_id, c.old_root_id, c.new_root_id) == (
        "B", "B", "R", "B",
    )


def test_cleanup_delete_entire_tree_leaves_no_impacts():
    parent_of = {"R": None, "A": "R", "B": "A"}
    plan = plan_retention_cleanup(
        parent_of, expired_ids={"R", "A", "B"}, protected_ids=set()
    )
    assert plan.deleted_ids == ["A", "B", "R"]
    assert plan.impacts == []


def test_cleanup_protected_expired_messages_are_not_deleted():
    # R(到期但冻结) -> A(到期)；保护 R 后 A 删除，A 的子节点无；R 存活
    parent_of = {"R": None, "A": "R", "B": "A"}
    plan = plan_retention_cleanup(
        parent_of, expired_ids={"R", "A"}, protected_ids={"R"}
    )
    assert plan.deleted_ids == ["A"]
    assert plan.protected_expired_ids == frozenset({"R"})
    impacts = _impact_map(plan)
    # B 重挂到被保护而存活的 R；根仍是 R
    b = impacts["B"]
    assert (b.old_parent_id, b.new_parent_id, b.old_root_id, b.new_root_id) == (
        "A", "R", "R", "R",
    )


def test_cleanup_deletes_disjoint_subtrees_independently():
    # 两棵独立树：R1->A(到期)->D；R2(到期)->E
    parent_of = {"R1": None, "A": "R1", "D": "A", "R2": None, "E": "R2"}
    plan = plan_retention_cleanup(
        parent_of, expired_ids={"A", "R2"}, protected_ids=set()
    )
    assert plan.deleted_ids == ["A", "R2"]
    impacts = _impact_map(plan)
    # D 重挂 R1，根不变
    assert impacts["D"].new_parent_id == "R1"
    assert (impacts["D"].old_root_id, impacts["D"].new_root_id) == ("R1", "R1")
    # E 失去根 R2，自成新根
    assert (impacts["E"].old_parent_id, impacts["E"].new_parent_id) == ("R2", None)
    assert (impacts["E"].old_root_id, impacts["E"].new_root_id) == ("R2", "E")


def test_cleanup_without_expired_messages_is_noop():
    parent_of = {"R": None, "A": "R"}
    plan = plan_retention_cleanup(parent_of, expired_ids=set(), protected_ids=set())
    assert plan.deleted_ids == [] and plan.impacts == []


def test_cleanup_expired_foreign_ids_are_ignored():
    parent_of = {"R": None}
    plan = plan_retention_cleanup(
        parent_of, expired_ids={"ghost", "R"}, protected_ids={"ghost2"}
    )
    assert plan.deleted_ids == ["R"]
    assert plan.expired_ids == frozenset({"R"})
    assert plan.protected_expired_ids == frozenset()
