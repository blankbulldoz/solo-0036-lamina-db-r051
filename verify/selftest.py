"""Differential self-tests for the independent reference model.

These checks do not touch the database.  They compare the model against a
second, deliberately naive implementation of the same documented rules and
against hand-computed expectations.  If the oracle itself were wrong, a
"passing" verification would be meaningless, so the oracle needs its own
independent evidence.
"""

from __future__ import annotations

import json

from . import journal, model


class SelfTestFailure(AssertionError):
    pass


def _require(condition, message):
    if not condition:
        raise SelfTestFailure(message)


def _relation_record(sequence=1, name="sp"):
    return {
        "format": 1, "sequence": sequence, "kind": "relation",
        "relation": {
            "name": name, "primary": "id", "created": sequence,
            "columns": [
                {"name": "id", "type": "int", "nullable": False},
                {"name": "v", "type": "int", "nullable": True},
                {"name": "t", "type": "text", "nullable": True},
            ],
        },
    }


def _commit(sequence, tx, *mutations):
    return {"format": 1, "sequence": sequence, "kind": "commit",
            "transaction": tx, "writes": list(mutations)}


def _put(relation, key, value, text=None):
    return {"relation": relation, "key": key,
            "row": {"id": _id(key), "v": value, "t": text}}


def _id(key_token):
    return int(key_token.split(":", 1)[1])


def _delete(relation, key):
    return {"relation": relation, "key": key, "deleted": True}


def _naive_scan(records, relation, at):
    """Second implementation: replay from scratch, no incremental state."""
    latest = {}
    for record in records:
        if record["sequence"] > at:
            break
        if record.get("kind") != "commit":
            continue
        for mutation in record.get("writes") or ():
            if mutation["relation"] != relation:
                continue
            if mutation.get("deleted"):
                latest.pop(mutation["key"], None)
            else:
                latest[mutation["key"]] = mutation.get("row")
    return latest


def test_snapshot_agrees_with_naive_replay():
    records = [
        _relation_record(),
        _commit(2, "a", _put("sp", "int:1", 10), _put("sp", "int:2", 20)),
        _commit(3, "b", _put("sp", "int:1", 30)),
        _commit(4, "c", _delete("sp", "int:2"), _put("sp", "int:3", 40)),
        _commit(5, "d", _put("sp", "int:1", 50), _put("sp", "int:2", 60)),
    ]
    history = model.build(records)
    for at in range(1, 6):
        _require(history.scan("sp", at) == _naive_scan(records, "sp", at),
                 "snapshot at %d diverges from naive replay" % at)


def test_retention_matches_bruteforce():
    """For every horizon, a version is kept iff some snapshot needs it.

    Independent definition: a version v of key k is essential iff there exists
    a snapshot ``at`` in ``horizon..head`` for which ``v`` is the visible
    version of ``k``; *or* it is the newest version at/before horizon (the
    base a later scan starts from).  A correct vacuum keeps exactly those.
    """
    records = [
        _relation_record(),
        _commit(2, "a", _put("sp", "int:1", 10)),
        _commit(3, "b", _put("sp", "int:1", 20)),
        _commit(4, "c", _put("sp", "int:1", 30)),
        _commit(5, "d", _delete("sp", "int:1")),
        _commit(6, "e", _put("sp", "int:1", 40)),
        _commit(7, "f", _put("sp", "int:2", 99)),
    ]
    for horizon in range(2, 8):
        partial = model.build(records)
        kept = partial.retained_at(horizon)
        expected = _essential_versions(records, "sp", horizon)
        got = {(token[1], version["sequence"])
               for token, chain in kept.items()
               for version in chain}
        _require(got == expected,
                 "horizon %d kept %r, essential set %r" % (horizon, got, expected))


def _essential_versions(records, relation, horizon):
    head = records[-1]["sequence"]
    chains = {}
    for record in records:
        if record.get("kind") != "commit":
            continue
        for mutation in record.get("writes") or ():
            if mutation["relation"] != relation:
                continue
            chains.setdefault(mutation["key"], []).append({
                "sequence": record["sequence"],
                "row": None if mutation.get("deleted") else mutation.get("row"),
                "deleted": bool(mutation.get("deleted")),
            })
    essential = set()
    for token, chain in chains.items():
        # newest at or before horizon is the base for later scans
        base = None
        for version in chain:
            if version["sequence"] <= horizon:
                base = version
        if base is not None:
            essential.add((token, base["sequence"]))
        # every version that is visible at some snapshot in the interval
        for at in range(horizon, head + 1):
            visible = None
            for version in chain:
                if version["sequence"] <= at:
                    visible = version
            if visible is not None and visible["sequence"] >= horizon:
                essential.add((token, visible["sequence"]))
        # a live newest version must remain even with no later snapshot
        if chain and chain[-1]["sequence"] >= horizon:
            essential.add((token, chain[-1]["sequence"]))
    return {pair for pair in essential if pair[1] > 0}


def test_tombstone_drops_key_at_or_below_horizon():
    records = [
        _relation_record(),
        _commit(2, "a", _put("sp", "int:1", 10)),
        _commit(3, "b", _delete("sp", "int:1")),
    ]
    history = model.build(records)
    # Tombstone at sequence 3.  Below the tombstone the key is still live and
    # must be retained; once the horizon reaches the tombstone the whole key
    # is unnecessary.
    _require(history.expected_space(2)[0] == 1,
             "live history before the tombstone must be retained")
    _require(history.expected_space(3)[0] == 0,
             "final tombstone at the horizon should remove the whole key")


def test_horizon_never_regresses():
    records = [
        _relation_record(),
        _commit(2, "a", _put("sp", "int:1", 10)),
        {"format": 1, "sequence": 3, "kind": "vacuum", "horizon": 2},
        {"format": 1, "sequence": 4, "kind": "vacuum", "horizon": 1},
    ]
    try:
        model.build(records)
    except model.OracleError:
        return
    raise SelfTestFailure("regressing horizon was accepted")


def test_horizon_ahead_of_head_rejected():
    records = [
        _relation_record(),
        {"format": 1, "sequence": 2, "kind": "vacuum", "horizon": 9},
    ]
    try:
        model.build(records)
    except model.OracleError:
        return
    raise SelfTestFailure("horizon ahead of head was accepted")


def test_journal_parser_roundtrip():
    """Encode frames by hand, parse them back, and check the boundary cases."""
    import struct
    import zlib

    header = b"LMW1" + struct.pack(">Q", 0)
    header += struct.pack(">I", zlib.crc32(header) & 0xFFFFFFFF)
    payload = json.dumps(_relation_record(1)).encode()
    frame = struct.pack(">I", len(payload)) + struct.pack(
        ">I", zlib.crc32(payload) & 0xFFFFFFFF) + payload
    log = journal.parse_bytes(header + frame)
    _require(log.base == 0 and len(log.records) == 1, "clean frame did not parse")
    _require(log.records[0]["kind"] == "relation", "record content changed")

    # incomplete tail is reported, not an error
    tailed = journal.parse_bytes(header + frame + b"\x00\x00\x00")
    _require(tailed.tail_bytes == 3, "incomplete tail not reported")
    _require(len(tailed.records) == 1, "complete frame lost near tail")

    # a flipped payload byte is a hard error
    broken = bytearray(header + frame)
    broken[-1] ^= 1
    try:
        journal.parse_bytes(bytes(broken))
    except journal.JournalError:
        pass
    else:
        raise SelfTestFailure("corrupted frame was accepted")

    # a broken header checksum is a hard error
    bad_header = bytearray(header)
    bad_header[5] ^= 1
    try:
        journal.parse_bytes(bytes(bad_header + frame))
    except journal.JournalError:
        pass
    else:
        raise SelfTestFailure("corrupted header was accepted")


def test_index_entries_model():
    records = [
        _relation_record(),
        _commit(2, "a", _put("sp", "int:1", 10, "x")),
        _commit(3, "b", _put("sp", "int:1", 20, "y")),
        _commit(4, "c", _put("sp", "int:2", 10, "z")),
    ]
    history = model.build(records)
    # before vacuum: two historical values for key 1, one for key 2
    _require(history.expected_index_entries("v") == 3,
             "index entries before vacuum: %r" % history.expected_index_entries("v"))
    history = model.build(records + [
        {"format": 1, "sequence": 5, "kind": "vacuum", "horizon": 4}])
    _require(history.expected_index_entries("v") == 2,
             "index entries after vacuum: %r" % history.expected_index_entries("v"))


def test_value_tokens():
    cases = [(-3, "int:-3"), (0, "int:0"), (2 ** 62, "int:%d" % 2 ** 62),
             ("x", 'text:"x"'), (True, "bool:true"), (False, "bool:false"),
             (None, "null:null")]
    for value, token in cases:
        got = model.json_key(value)
        _require(got == token,
                 "token %r -> %r, expected %r" % (value, got, token))
    # bool must not be confused with int: Python treats True == 1 as dict keys,
    # so check the two tokens explicitly rather than relying on dict lookup.
    _require(model.json_key(True) != model.json_key(1), "bool/int tokens collide")
    _require(model.json_key(False) != model.json_key(0), "bool/int tokens collide")


def test_key_token_roundtrip():
    from .harness import token_key
    for value in (-5, 0, 12, "abc", 'quote"d', True, False):
        token = model.json_key(value)
        _require(token_key(token) == value,
                 "roundtrip %r -> %r -> %r" % (value, token, token_key(token)))


TESTS = [
    test_snapshot_agrees_with_naive_replay,
    test_retention_matches_bruteforce,
    test_tombstone_drops_key_at_or_below_horizon,
    test_horizon_never_regresses,
    test_horizon_ahead_of_head_rejected,
    test_journal_parser_roundtrip,
    test_index_entries_model,
    test_value_tokens,
    test_key_token_roundtrip,
]


def main(binary=None, verbose=False, as_json=False):
    failures = []
    for test in TESTS:
        try:
            test()
            if verbose:
                print("PASS %s" % test.__name__)
        except SelfTestFailure as exc:
            failures.append({"test": test.__name__, "error": str(exc)})
            if verbose:
                print("FAIL %s: %s" % (test.__name__, exc))
    if as_json:
        print(json.dumps({"tests": len(TESTS), "failures": failures}, indent=2))
    else:
        print("selftest: %d/%d passed" % (len(TESTS) - len(failures), len(TESTS)))
        for failure in failures:
            print("  FAIL %(test)s: %(error)s" % failure)
    return 0 if not failures else 1
