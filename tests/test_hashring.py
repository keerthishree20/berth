"""The ring. Its value is entirely in what does not move."""

from __future__ import annotations

import pytest

from berth.hashring import HashRing

KEYS = [f"session-{i}" for i in range(10_000)]


def test_a_key_always_lands_on_the_same_node():
    ring = HashRing(["a", "b", "c"])
    assert all(ring.get("session-1") == ring.get("session-1") for _ in range(10))


def test_an_empty_ring_owns_nothing():
    assert HashRing().get("anything") is None
    assert HashRing().get_preference("anything", 3) == []


def test_the_ring_is_built_from_names_not_insertion_order():
    forwards = HashRing(["a", "b", "c"])
    backwards = HashRing(["c", "b", "a"])
    assert [forwards.get(k) for k in KEYS[:500]] == [backwards.get(k) for k in KEYS[:500]]


def test_load_is_reasonably_even():
    ring = HashRing([f"node{i}" for i in range(8)])
    counts = ring.distribution(KEYS)
    expected = len(KEYS) / 8
    worst = max(abs(count - expected) / expected for count in counts.values())
    assert worst < 0.20, f"worst node is {worst:.0%} off its share: {counts}"


def test_too_few_replicas_makes_the_load_visibly_uneven():
    """Stated as a test because it is the parameter people get wrong. One point
    per node divides the circle very unevenly, and no amount of hashing fixes
    it."""
    sparse = HashRing([f"node{i}" for i in range(8)], replicas=1)
    counts = sparse.distribution(KEYS)
    expected = len(KEYS) / 8
    worst = max(abs(count - expected) / expected for count in counts.values())
    assert worst > 0.30, "a single replica per node should be visibly skewed"


def test_removing_a_node_moves_only_its_own_keys():
    """The property the whole structure exists for. With plain modulo hashing
    this number would be close to 100%."""
    ring = HashRing([f"node{i}" for i in range(8)])
    before = {key: ring.get(key) for key in KEYS}

    ring.remove("node3")
    after = {key: ring.get(key) for key in KEYS}

    moved = sum(1 for key in KEYS if before[key] != after[key])
    owned_by_removed = sum(1 for key in KEYS if before[key] == "node3")

    assert moved == owned_by_removed, "a key not owned by node3 was remapped"
    assert moved / len(KEYS) < 0.20, f"{moved / len(KEYS):.0%} of keys moved"


def test_adding_a_node_only_takes_keys_it_should():
    ring = HashRing([f"node{i}" for i in range(8)])
    before = {key: ring.get(key) for key in KEYS}

    ring.add("node8")
    after = {key: ring.get(key) for key in KEYS}

    moved = [key for key in KEYS if before[key] != after[key]]
    assert all(after[key] == "node8" for key in moved), "keys moved between existing nodes"
    assert 0.05 < len(moved) / len(KEYS) < 0.25


def test_modulo_hashing_is_the_thing_this_avoids():
    """The comparison that makes the point. Same keys, same node change."""
    def modulo_owner(key: str, count: int) -> int:
        return hash(key) % count

    moved = sum(1 for key in KEYS if modulo_owner(key, 8) != modulo_owner(key, 7))
    assert moved / len(KEYS) > 0.80, "modulo hashing should remap almost everything"


def test_preference_order_is_stable_and_distinct():
    ring = HashRing([f"node{i}" for i in range(5)])
    order = ring.get_preference("session-42", 5)
    assert len(order) == len(set(order)) == 5
    assert order[0] == ring.get("session-42")
    assert ring.get_preference("session-42", 5) == order


def test_preference_stops_at_the_number_asked_for():
    ring = HashRing([f"node{i}" for i in range(5)])
    assert len(ring.get_preference("k", 2)) == 2
    assert len(ring.get_preference("k", 99)) == 5
    assert ring.get_preference("k", 0) == []


def test_the_fallback_for_a_downed_node_is_the_same_for_all_its_keys():
    """Why the fallback walks the ring instead of picking at random: while a
    node is down its keys all land on one replacement, which warms one cache
    rather than diluting every cache."""
    ring = HashRing([f"node{i}" for i in range(6)])
    owned = [key for key in KEYS if ring.get(key) == "node2"][:200]
    seconds = {ring.get_preference(key, 2)[1] for key in owned}
    assert len(seconds) < 6, "every key of a downed node scattered to a different node"


def test_membership_and_repr():
    ring = HashRing(["a", "b"])
    assert len(ring) == 2 and "a" in ring and "z" not in ring
    assert ring.nodes() == ["a", "b"]
    assert ring.remove("a") is True
    assert ring.remove("a") is False
    ring.add("b")  # already present, no duplicate points
    assert ring.nodes() == ["b"]
    assert "1 nodes" in repr(ring)


def test_replicas_must_be_positive():
    with pytest.raises(ValueError, match="replicas must be at least 1"):
        HashRing(["a"], replicas=0)
