"""Fixed, reviewable interleavings for the two target workflows.

Each scenario is a hand-chosen schedule that isolates one state-machine
edge.  They are deliberately *not* random: they are the smallest schedules
that discriminate a correct executor from the specific failure modes we
care about, and a maintenance engineer can read them top to bottom.

The failure mode the brief highlights -- "the same final row set can hide a
mid-flight visibility leak" -- is addressed by making every intermediate
step observable: the harness audits the whole retained interval, all active
transactions and the durable journal after *every* operation, not only at
the end.
"""

from __future__ import annotations

from .harness import VerificationFailure

# A short band of text values reused by scenarios; kept small so a trace is
# readable when a failure is reported.
TEXT = ("alpha", "beta", "gamma", "delta", "epsilon")


def _seed_rows(case, rows):
    """Commit one batch of rows as a single fixture transaction."""
    tx = case.begin()
    for key, value in rows:
        case.put(tx, key, value, TEXT[key % len(TEXT)])
    case.commit(tx)
    return tx


# ---------------------------------------------------------------------------
# Workflow A: promised versions survive an active transaction's horizon.
# ---------------------------------------------------------------------------

def scenario_promise_blocks_reclamation(case):
    """An update strands the old version; an active reader keeps it alive."""
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20), (3, 30)])

    reader = case.begin(read_only=True)          # pins snapshot at head
    pinned_snapshot = case.active[reader]["snapshot"]

    writer = case.begin()
    case.put(writer, 1, 111, TEXT[1])            # key 1 now has a history
    case.put(writer, 2, 222, TEXT[2])
    case.commit(writer)

    # Ask for a future horizon; it must be clamped to the reader's snapshot.
    code, result = case.vacuum(case.oracle.head)
    if result["horizon"] != pinned_snapshot:
        raise VerificationFailure(
            "scenario.promise-horizon",
            "vacuum crossed the active reader: horizon %s != pinned %s" % (
                result["horizon"], pinned_snapshot))
    if not result["pinned"]:
        raise VerificationFailure("scenario.promise-pinned", "active reader did not pin")

    # The whole point: the reader still sees its pre-update rows.
    outward = case.server.query({"relation": "sp", "transaction": reader})
    if outward[0] != 200:
        raise VerificationFailure(
            "scenario.promise-read", "reader lost its snapshot: %r" % (outward[1],))
    got = {row["id"]: (row["v"], row["t"]) for row in outward[1]["rows"]}
    want = {1: (10, TEXT[1]), 2: (20, TEXT[2]), 3: (30, TEXT[3])}
    if got != want:
        raise VerificationFailure(
            "scenario.promise-rows", "reader saw %r, promised %r" % (got, want))

    # Closing the reader releases the pin; the *next* vacuum may advance.
    case.rollback(reader)
    code, released = case.vacuum(case.oracle.head)
    if released["pinned"]:
        raise VerificationFailure("scenario.release-pinned", "vacuum still pinned after close")
    if released["horizon"] < pinned_snapshot:
        raise VerificationFailure(
            "scenario.release-horizon", "released horizon %s < pinned %s" % (
                released["horizon"], pinned_snapshot))

    # The pre-update snapshot is now genuinely reclaimed.
    below = case.server.query({"relation": "sp", "at": pinned_snapshot})
    if below[0] != 410:
        raise VerificationFailure(
            "scenario.reclaim-not-enforced",
            "reclaimed snapshot %s still readable: %s" % (pinned_snapshot, below[0]))
    return {"pinned": result, "released": released}


def scenario_multiple_readers_lowest_wins(case):
    """With several readers the oldest snapshot, not the newest, pins."""
    case.create_relation()
    _seed_rows(case, [(1, 10)])
    oldest = case.begin(read_only=True)
    oldest_snapshot = case.active[oldest]["snapshot"]

    writer = case.begin()
    case.put(writer, 1, 100, TEXT[0])
    case.commit(writer)

    newer = case.begin(read_only=True)
    newer_snapshot = case.active[newer]["snapshot"]
    if newer_snapshot <= oldest_snapshot:
        raise VerificationFailure(
            "scenario.snapshots-not-distinct",
            "readers did not capture distinct snapshots %s/%s" % (
                oldest_snapshot, newer_snapshot))

    code, result = case.vacuum(case.oracle.head)
    if result["horizon"] != oldest_snapshot:
        raise VerificationFailure(
            "scenario.lowest-wins",
            "horizon %s, oldest reader snapshot %s" % (result["horizon"], oldest_snapshot))

    # Close the oldest first: the newer reader must now be the limiting one.
    case.rollback(oldest)
    code, second = case.vacuum(case.oracle.head)
    if second["horizon"] != newer_snapshot:
        raise VerificationFailure(
            "scenario.next-lowest",
            "after closing oldest reader horizon %s != newer %s" % (
                second["horizon"], newer_snapshot))
    if not second["pinned"]:
        raise VerificationFailure("scenario.next-pinned", "remaining reader did not pin")
    case.rollback(newer)
    return {"first": result, "second": second}


def scenario_tombstone_and_history(case):
    """Deletes leave tombstones that a reader still needs, then vanish."""
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20), (3, 30)])
    reader = case.begin(read_only=True)
    pinned = case.active[reader]["snapshot"]

    deleter = case.begin()
    case.delete(deleter, 1)                     # tombstone for key 1
    case.put(deleter, 2, 200, TEXT[2])          # update leaves a history
    case.put(deleter, 4, 40, TEXT[4])           # brand new key
    case.commit(deleter)

    code, result = case.vacuum(case.oracle.head)
    if result["horizon"] != pinned:
        raise VerificationFailure(
            "scenario.tombstone-pin", "horizon %s != pinned %s" % (result["horizon"], pinned))

    # The pinned reader must still see key 1 (pre-delete) and old key 2.
    outward = case.server.query({"relation": "sp", "transaction": reader})
    got = {row["id"]: row["v"] for row in outward[1]["rows"]}
    want = {1: 10, 2: 20, 3: 30}
    if got != want:
        raise VerificationFailure(
            "scenario.tombstone-reader", "reader saw %r, promised %r" % (got, want))

    case.rollback(reader)
    code, released = case.vacuum(case.oracle.head)
    assert_not_readable(case, pinned)
    head_rows = case.server.query({"relation": "sp"})[1]["rows"]
    if {row["id"]: row["v"] for row in head_rows} != {2: 200, 3: 30, 4: 40}:
        raise VerificationFailure(
            "scenario.tombstone-head", "head set wrong: %r" % (head_rows,))
    return {"pinned": result, "released": released}


def scenario_tombstone_dropped_when_horizon_passes(case):
    """A final tombstone at or below the horizon removes the whole key."""
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20)])
    deleter = case.begin()
    case.delete(deleter, 1)
    case.commit(deleter)
    before = case.server.stats()
    if before["keys"] != 2:
        raise VerificationFailure(
            "scenario.tombstone-key-count", "keys %s, expected 2" % (before["keys"],))
    code, result = case.vacuum(case.oracle.head)
    after = case.server.stats()
    if after["keys"] != 1:
        raise VerificationFailure(
            "scenario.tombstone-not-dropped",
            "deleted key still retained after horizon: keys %s" % (after["keys"],))
    if result["removed"] != 2:
        raise VerificationFailure(
            "scenario.tombstone-removed", "removed %s, expected 2 versions" % (result["removed"],))
    return {"vacuum": result, "keys": after["keys"]}


# ---------------------------------------------------------------------------
# Workflow B: closed transactions stop affecting others.
# ---------------------------------------------------------------------------

def scenario_rollback_leaves_no_trace(case):
    """A rolled-back private write never reaches the journal or other readers."""
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20)])
    head_before = case.oracle.head
    journal_before = list(case.journal.records)

    tx = case.begin()
    case.put(tx, 1, 999, TEXT[1])       # would overwrite a committed row
    case.put(tx, 5, 50, TEXT[0])        # would insert a new row
    case.delete(tx, 2)                  # would delete a committed row
    case.rollback(tx)

    if case.oracle.head != head_before:
        raise VerificationFailure(
            "scenario.rollback-head", "rollback advanced head %s -> %s" % (
                head_before, case.oracle.head))
    if case.journal.records != journal_before:
        raise VerificationFailure(
            "scenario.rollback-journal", "rollback wrote durable records")
    current = case.server.query({"relation": "sp"})[1]["rows"]
    if {row["id"]: row["v"] for row in current} != {1: 10, 2: 20}:
        raise VerificationFailure(
            "scenario.rollback-leak", "rolled-back writes visible: %r" % (current,))
    return {"head": case.oracle.head}


def scenario_conflict_discards_all_writes(case):
    """The loser of first-committer-wins publishes none of its other writes."""
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20)])

    winner = case.begin()
    loser = case.begin()
    case.put(winner, 1, 111, TEXT[1])   # shared key
    case.put(winner, 3, 30, TEXT[3])    # winner-only key
    case.put(loser, 1, 222, TEXT[2])    # conflicts on key 1
    case.put(loser, 4, 40, TEXT[4])     # would be a clean insert
    case.commit(winner)

    head_after_winner = case.oracle.head
    code, result = case.commit(loser)
    if code != 409:
        raise VerificationFailure(
            "scenario.conflict-not-detected", "loser commit returned %s" % (code,))
    if case.oracle.head != head_after_winner:
        raise VerificationFailure(
            "scenario.conflict-head", "losing commit advanced the sequence")
    rows = case.server.query({"relation": "sp"})[1]["rows"]
    observed = {row["id"]: row["v"] for row in rows}
    if observed != {1: 111, 2: 20, 3: 30}:
        raise VerificationFailure(
            "scenario.conflict-partial", "losing writes leaked: %r" % (observed,))
    return {"head": case.oracle.head, "rows": observed}


def scenario_conflict_after_vacuum(case):
    """First-committer-wins still works when the loser's chain was pruned."""
    case.create_relation()
    _seed_rows(case, [(1, 10)])
    # Make two reader-free generations, then reclaim so only the newest stays.
    updater = case.begin()
    case.put(updater, 1, 20, TEXT[1])
    case.commit(updater)
    case.vacuum(case.oracle.head)

    slow = case.begin()                 # snapshot at current head
    fast = case.begin()
    case.put(slow, 1, 300, TEXT[2])
    case.put(fast, 1, 400, TEXT[3])
    case.commit(fast)
    code, result = case.commit(slow)
    if code != 409:
        raise VerificationFailure(
            "scenario.post-vacuum-conflict", "conflict not detected after vacuum")
    rows = case.server.query({"relation": "sp"})[1]["rows"]
    if rows != [{"id": 1, "v": 400, "t": TEXT[3]}]:
        raise VerificationFailure(
            "scenario.post-vacuum-rows", "unexpected head state %r" % (rows,))
    return {"head": case.oracle.head}


def scenario_private_writes_visible_only_to_owner(case):
    """Read-your-writes is private; another open transaction cannot see it."""
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20)])
    writer = case.begin()
    observer = case.begin()             # same snapshot, separate private set
    case.put(writer, 1, 777, TEXT[1])
    case.put(writer, 6, 60, TEXT[0])

    private = case.server.query({"relation": "sp", "transaction": writer})[1]["rows"]
    if {row["id"]: row["v"] for row in private} != {1: 777, 2: 20, 6: 60}:
        raise VerificationFailure(
            "scenario.own-writes", "writer did not read own writes: %r" % (private,))
    observed = case.server.query({"relation": "sp", "transaction": observer})[1]["rows"]
    if {row["id"]: row["v"] for row in observed} != {1: 10, 2: 20}:
        raise VerificationFailure(
            "scenario.private-leak", "other transaction saw private writes: %r" % (observed,))

    case.rollback(writer)
    # After the writer rolls back, nothing it did survives for the observer.
    after = case.server.query({"relation": "sp", "transaction": observer})[1]["rows"]
    if {row["id"]: row["v"] for row in after} != {1: 10, 2: 20}:
        raise VerificationFailure(
            "scenario.private-rollback", "observer state changed: %r" % (after,))
    case.rollback(observer)
    return {"rows": len(after)}


def scenario_commit_then_snapshot_isolation(case):
    """A snapshot taken before a commit never sees that commit, even after it."""
    case.create_relation()
    _seed_rows(case, [(1, 10)])
    early = case.begin(read_only=True)
    early_snapshot = case.active[early]["snapshot"]

    writer = case.begin()
    case.put(writer, 1, 500, TEXT[1])
    case.put(writer, 7, 70, TEXT[2])
    case.commit(writer)

    late = case.begin(read_only=True)
    early_rows = case.server.query({"relation": "sp", "transaction": early})[1]["rows"]
    if {row["id"]: row["v"] for row in early_rows} != {1: 10}:
        raise VerificationFailure(
            "scenario.isolation-early", "early snapshot saw later commit: %r" % (early_rows,))
    late_rows = case.server.query({"relation": "sp", "transaction": late})[1]["rows"]
    if {row["id"]: row["v"] for row in late_rows} != {1: 500, 7: 70}:
        raise VerificationFailure(
            "scenario.isolation-late", "late snapshot missed commit: %r" % (late_rows,))

    # And vacuum framed by the early reader keeps the early answer intact.
    code, result = case.vacuum(case.oracle.head)
    if result["horizon"] != early_snapshot:
        raise VerificationFailure(
            "scenario.isolation-pin", "horizon %s != early snapshot %s" % (
                result["horizon"], early_snapshot))
    case.rollback(early)
    case.rollback(late)
    return {"early": early_snapshot}


def scenario_commit_does_not_hide_leaked_midstate(case):
    """Final row sets can coincide; the audit still separates the schedules.

    Two schedules reach the *same* head row set.  One commits an intermediate
    value through a reader, the other never does.  A clean final comparison
    cannot tell them apart; the per-step retained-interval audit can.
    """
    case.create_relation()
    _seed_rows(case, [(1, 10)])
    reader = case.begin(read_only=True)         # pins the 10 version
    writer = case.begin()
    case.put(writer, 1, 20, TEXT[1])
    case.commit(writer)
    back = case.begin()
    case.put(back, 1, 10, TEXT[0])              # overwrite back to the original
    case.commit(back)

    final = case.server.query({"relation": "sp"})[1]["rows"]
    if {row["id"]: row["v"] for row in final} != {1: 10}:
        raise VerificationFailure("scenario.collinear-head", "head not restored")

    # The reader exists precisely so the intermediate 20 stays promised; the
    # harness will have verified it after every step.  Prove it directly too.
    seen = case.server.query({"relation": "sp", "transaction": reader})[1]["rows"]
    if {row["id"]: row["v"] for row in seen} != {1: 10}:
        raise VerificationFailure(
            "scenario.collinear-reader", "reader view changed: %r" % (seen,))
    intermediate = case.server.query({"relation": "sp", "at": 3})
    if intermediate[0] != 200 or intermediate[1]["rows"][0]["v"] != 20:
        raise VerificationFailure(
            "scenario.collinear-intermediate", "intermediate version lost: %r" % (intermediate,))
    case.rollback(reader)
    return {"head": case.oracle.head}


def scenario_closed_transactions_are_terminal(case):
    """Every close reason yields 404 on every follow-up operation."""
    case.create_relation()
    _seed_rows(case, [(1, 10)])

    # committed
    tx = case.begin()
    case.put(tx, 2, 20, TEXT[2])
    case.commit(tx)
    # conflict
    a = case.begin()
    b = case.begin()
    case.put(a, 1, 100, TEXT[0])
    case.put(b, 1, 200, TEXT[1])
    case.commit(a)
    code, _ = case.commit(b)
    if code != 409:
        raise VerificationFailure("scenario.terminal-conflict", "expected 409, got %s" % (code,))
    # rollback
    rolled = case.begin()
    case.put(rolled, 3, 30, TEXT[3])
    case.rollback(rolled)

    for closed in (tx, a, b, rolled):
        # The harness's own closed-transaction probe runs after every close;
        # reasserting here documents the requirement at the scenario level.
        for method, path, body in (
            ("POST", "/query", {"relation": "sp", "transaction": closed}),
            ("POST", "/transactions/%s/commit" % closed, {}),
            ("POST", "/transactions/%s/rollback" % closed, {}),
            ("POST", "/transactions/%s/put" % closed,
             {"relation": "sp", "row": {"id": 9, "v": 90, "t": None}}),
        ):
            code, payload = case.server.try_call(method, path, body)
            if code != 404:
                raise VerificationFailure(
                    "scenario.terminal-reuse", "closed tx %s accepted %s %s -> %s" % (
                        closed, method, path, code))
    stats = case.server.stats()
    if stats["active"]:
        raise VerificationFailure(
            "scenario.terminal-active", "closed transactions left pins: %r" % (stats["active"],))
    return {"head": case.oracle.head}


def scenario_restore_preserves_horizon_and_history(case, binary, temp_root):
    """A restored image keeps the reclaimed horizon and the retained versions.

    Reclamation is part of the durable state a backup freezes: the restored
    instance must expose exactly the same retained interval, and its change
    history must begin at the image's base (stale cursors get 410).
    """
    import pathlib

    from .harness import Case

    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20), (3, 30)])
    updater = case.begin()
    case.put(updater, 1, 100, TEXT[0])
    case.put(updater, 2, 200, TEXT[1])
    case.commit(updater)
    code, vac = case.vacuum(case.oracle.head)
    horizon = vac["horizon"]
    head = case.oracle.head
    retained_before = {
        at: {row["id"]: row["v"]
             for row in case.server.query({"relation": "sp", "at": at})[1]["rows"]}
        for at in range(horizon, head + 1)
    }

    backup_path = pathlib.Path(temp_root) / "portable.lmi"
    backup_path.write_bytes(case.server.backup())

    restored = Case(binary, pathlib.Path(temp_root) / "restored", verbose=False)
    restored.start(restore=backup_path)
    try:
        stats = restored.server.stats()
        if stats["horizon"] != horizon:
            raise VerificationFailure(
                "scenario.restore-horizon",
                "restored horizon %s != %s" % (stats["horizon"], horizon))
        if stats["sequence"] != head:
            raise VerificationFailure(
                "scenario.restore-head", "restored head %s != %s" % (
                    stats["sequence"], head))
        for at in range(horizon, head + 1):
            code, payload = restored.server.query({"relation": "sp", "at": at})
            if code != 200:
                raise VerificationFailure(
                    "scenario.restore-snapshot",
                    "restored snapshot %s unreadable: %r" % (at, payload))
            observed = {row["id"]: row["v"] for row in payload["rows"]}
            if observed != retained_before[at]:
                raise VerificationFailure(
                    "scenario.restore-rows", "at=%s restored %r != source %r" % (
                        at, observed, retained_before[at]))
        code, _ = restored.server.query({"relation": "sp", "at": horizon - 1})
        if code != 410:
            raise VerificationFailure(
                "scenario.restore-reclaim", "pre-horizon snapshot readable after restore")
        code, payload = restored.server.changes(after=0, limit=10)
        if code != 410:
            raise VerificationFailure(
                "scenario.restore-changes", "stale change cursor accepted after restore: %s" % (
                    code,))
    finally:
        restored.stop()
    return {"horizon": horizon, "head": head}


def scenario_restore_with_live_reader(case, binary, temp_root):
    """A backup taken while a reader pins the horizon freezes that pin.

    The restored instance has no active transactions (sessions are not
    durable), so its horizon equals the frozen one and may advance from
    there.  What must hold is that no version promised at backup time was
    lost from the image.
    """
    import pathlib

    from .harness import Case

    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20)])
    reader = case.begin(read_only=True)
    pinned = case.active[reader]["snapshot"]
    writer = case.begin()
    case.put(writer, 1, 50, TEXT[1])
    case.commit(writer)
    # Vacuum must clamp to the reader; capture the promised snapshot.
    code, vac = case.vacuum(case.oracle.head)
    if vac["horizon"] != pinned:
        raise VerificationFailure(
            "scenario.restore-pin", "horizon %s != pinned %s" % (vac["horizon"], pinned))
    promised = {row["id"]: row["v"]
                for row in case.server.query({"relation": "sp", "transaction": reader})[1]["rows"]}

    backup_path = pathlib.Path(temp_root) / "pinned.lmi"
    backup_path.write_bytes(case.server.backup())
    case.rollback(reader)

    restored = Case(binary, pathlib.Path(temp_root) / "restored-pinned", verbose=False)
    restored.start(restore=backup_path)
    try:
        restored_horizon = restored.server.stats()["horizon"]
        if restored_horizon != pinned:
            raise VerificationFailure(
                "scenario.restore-pinned-horizon",
                "restored horizon %s != pinned %s" % (restored_horizon, pinned))
        code, payload = restored.server.query({"relation": "sp", "at": pinned})
        if code != 200:
            raise VerificationFailure(
                "scenario.restore-pinned-snapshot", "promised snapshot lost: %r" % (payload,))
        observed = {row["id"]: row["v"] for row in payload["rows"]}
        if observed != promised:
            raise VerificationFailure(
                "scenario.restore-pinned-rows", "restored %r != promised %r" % (
                    observed, promised))
    finally:
        restored.stop()
    return {"pinned": pinned}


def scenario_index_rebuilt_after_reclamation(case):
    """Historical index candidates survive until the horizon, then rebuild.

    The index keeps supersets of historical (value, primary) pairs; a
    reclaimed generation must disappear from both the index and the scan
    answer.  A stale candidate must never resurrect an old value.
    """
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20), (3, 30)])
    case.create_index("by_v", "v")

    # Two generations of key 1 so the index holds a superseded candidate.
    reader = case.begin(read_only=True)
    pinned = case.active[reader]["snapshot"]
    updater = case.begin()
    case.put(updater, 1, 999, TEXT[1])
    case.put(updater, 2, 888, TEXT[2])
    case.commit(updater)

    # Before reclamation the old candidates must still answer historical reads
    # through the index path (the harness compares index vs scan at every at).
    at_old = pinned
    code, payload = case.server.query({
        "relation": "sp", "at": at_old,
        "where": [{"column": "v", "op": "=", "value": 10}]})
    if code != 200 or not payload["rows"] or payload["rows"][0]["id"] != 1:
        raise VerificationFailure(
            "scenario.index-history", "historical index lookup lost the old value: %r" % (
                payload,))
    if payload["plan"]["access"] != "index":
        raise VerificationFailure(
            "scenario.index-path-not-used",
            "equality on an indexed column used %s" % (payload["plan"]["access"],))
    if payload["plan"]["candidates"] < 1:
        raise VerificationFailure(
            "scenario.index-candidates", "index supplied no candidates: %r" % (payload,))
    # Scanning would report more candidates than the index for this predicate;
    # if not, the index is not being used as a candidate source.
    code, by_scan = case.server.query({
        "relation": "sp", "at": at_old, "force_scan": True,
        "where": [{"column": "v", "op": "=", "value": 10}]})
    if by_scan["plan"]["candidates"] <= payload["plan"]["candidates"]:
        raise VerificationFailure(
            "scenario.index-candidate-economy",
            "index candidates %s not fewer than scan %s" % (
                payload["plan"]["candidates"], by_scan["plan"]["candidates"]))

    code, result = case.vacuum(case.oracle.head)
    if result["horizon"] != pinned:
        raise VerificationFailure(
            "scenario.index-pin", "horizon %s != pinned %s" % (result["horizon"], pinned))
    case.rollback(reader)
    code, result = case.vacuum(case.oracle.head)

    # The reclaimed value must no longer be a candidate anywhere.
    code, payload = case.server.query({
        "relation": "sp", "where": [{"column": "v", "op": "=", "value": 10}]})
    if payload["rows"]:
        raise VerificationFailure(
            "scenario.index-stale-candidate",
            "reclaimed value 10 still returned: %r" % (payload,))
    stats = case.server.stats()
    expected = case.oracle.expected_index_entries("v")
    actual = stats["index_entries"].get("by_v")
    if actual != expected:
        raise VerificationFailure(
            "scenario.index-entries", "index entries %s != model %s" % (actual, expected))
    return {"entries": actual, "horizon": result["horizon"]}


def scenario_index_and_closed_transactions(case):
    """A losing or rolled-back writer must not leave index candidates."""
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20)])
    case.create_index("by_v", "v")

    rolled = case.begin()
    case.put(rolled, 1, 777, TEXT[1])
    case.rollback(rolled)

    a = case.begin()
    b = case.begin()
    case.put(a, 1, 100, TEXT[0])
    case.put(b, 1, 200, TEXT[1])
    case.commit(a)
    code, _ = case.commit(b)
    if code != 409:
        raise VerificationFailure("scenario.index-conflict", "expected 409, got %s" % (code,))

    for value in (777, 200):
        code, payload = case.server.query({
            "relation": "sp", "where": [{"column": "v", "op": "=", "value": value}]})
        if payload["rows"]:
            raise VerificationFailure(
                "scenario.index-private-leak",
                "private/losing value %s leaked into the index: %r" % (value, payload))
    code, payload = case.server.query({
        "relation": "sp", "where": [{"column": "v", "op": "=", "value": 100}]})
    if [row["id"] for row in payload["rows"]] != [1]:
        raise VerificationFailure(
            "scenario.index-winner-missing", "winning value not indexed: %r" % (payload,))
    return {"head": case.oracle.head}


# ---------------------------------------------------------------------------
# Workflow C: durability of both together.
# ---------------------------------------------------------------------------

def scenario_vacuum_survives_restart(case):
    """A reclaimed horizon is durable and stays reclaimed, never secret."""
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20)])
    updater = case.begin()
    case.put(updater, 1, 11, TEXT[1])
    case.commit(updater)
    code, result = case.vacuum(case.oracle.head)
    horizon = result["horizon"]
    before = case.server.query({"relation": "sp"})[1]["rows"]

    case.restart()
    after = case.server.query({"relation": "sp"})[1]["rows"]
    if after != before:
        raise VerificationFailure(
            "scenario.restart-rows", "restart changed rows %r -> %r" % (before, after))
    stats = case.server.stats()
    if stats["horizon"] != horizon:
        raise VerificationFailure(
            "scenario.restart-horizon", "horizon %s != %s after restart" % (
                stats["horizon"], horizon))
    code, payload = case.server.query({"relation": "sp", "at": horizon - 1})
    if code != 410:
        raise VerificationFailure(
            "scenario.restart-reclaim", "reclaimed snapshot readable after restart: %s" % (code,))
    return {"horizon": horizon}


def scenario_checkpoint_and_vacuum(case):
    """Checkpoint carries the pruned store and horizon consistently."""
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20), (3, 30)])
    updater = case.begin()
    case.put(updater, 1, 100, TEXT[0])
    case.commit(updater)
    case.vacuum(case.oracle.head)
    case.checkpoint()
    before = case.server.query({"relation": "sp"})[1]["rows"]
    stats_before = case.server.stats()
    case.restart()
    after = case.server.query({"relation": "sp"})[1]["rows"]
    if after != before:
        raise VerificationFailure(
            "scenario.checkpoint-rows", "checkpoint recovery changed rows")
    stats_after = case.server.stats()
    for field in ("horizon", "keys", "versions"):
        if stats_after[field] != stats_before[field]:
            raise VerificationFailure(
                "scenario.checkpoint-%s" % field,
                "%s changed across checkpoint+restart: %s -> %s" % (
                    field, stats_before[field], stats_after[field]))
    return {"horizon": stats_after["horizon"]}


def assert_not_readable(case, at):
    code, payload = case.server.query({"relation": "sp", "at": at})
    if code != 410:
        raise VerificationFailure(
            "scenario.expected-reclaimed", "snapshot %s readable with %s" % (at, code))


def scenario_horizon_cannot_regress(case):
    """Once a horizon is durable it cannot be walked backwards."""
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20)])
    updater = case.begin()
    case.put(updater, 1, 11, TEXT[1])
    case.commit(updater)
    code, first = case.vacuum(case.oracle.head)
    if first["horizon"] <= 0:
        raise VerificationFailure("scenario.regress-no-advance", "horizon did not advance")
    durable = first["horizon"]
    lower = durable - 1
    # Every requested horizon below the durable one must be rejected.
    for requested in range(case.oracle.horizon):
        code, payload = case.server.vacuum(requested)
        if code != 400:
            raise VerificationFailure(
                "scenario.regress-accepted",
                "vacuum(%d) below durable horizon %d returned %s: %r" % (
                    requested, durable, code, payload))
    # The durable horizon is unchanged and the store still matches it.
    stats = case.server.stats()
    if stats["horizon"] != durable:
        raise VerificationFailure(
            "scenario.regress-state", "horizon moved to %s after rejected vacuums" % (
                stats["horizon"],))
    return {"horizon": durable, "rejected_below": durable}


def scenario_retained_counters_reflect_reality(case):
    """The removed/retained counters must match the observed store change."""
    case.create_relation()
    _seed_rows(case, [(1, 10), (2, 20), (3, 30)])
    updater = case.begin()
    case.put(updater, 1, 100, TEXT[0])
    case.put(updater, 2, 200, TEXT[1])
    case.commit(updater)
    before = case.server.stats()
    code, result = case.vacuum(case.oracle.head)
    after = case.server.stats()
    if before["versions"] - after["versions"] != result["removed"]:
        raise VerificationFailure(
            "scenario.counter-removed",
            "removed=%s but versions went %s -> %s" % (
                result["removed"], before["versions"], after["versions"]))
    if after["versions"] != result["retained"]:
        raise VerificationFailure(
            "scenario.counter-retained",
            "retained=%s but store holds %s versions" % (
                result["retained"], after["versions"]))
    if after["keys"] != case.oracle.expected_space(case.oracle.horizon)[0]:
        raise VerificationFailure(
            "scenario.counter-keys", "keys %s != model" % (after["keys"],))
    return {"removed": result["removed"], "retained": result["retained"]}


def scenario_no_lost_update_under_contention(case):
    """A commit that succeeded must never silently overwrite a newer commit."""
    case.create_relation()
    _seed_rows(case, [(1, 10)])
    stale = case.begin()                 # snapshot before the newer commit
    fresh = case.begin()
    case.put(fresh, 1, 20, TEXT[1])
    code, _ = case.commit(fresh)
    if code != 200:
        raise VerificationFailure("scenario.lost-update-setup", "fresh commit failed")
    case.put(stale, 1, 30, TEXT[2])      # stale writer now targets key 1
    code, payload = case.commit(stale)
    if code != 409:
        raise VerificationFailure(
            "scenario.lost-update", "stale writer committed over a newer version: %s" % (code,))
    rows = case.server.query({"relation": "sp"})[1]["rows"]
    if {row["id"]: row["v"] for row in rows} != {1: 20}:
        raise VerificationFailure(
            "scenario.lost-update-rows", "newer value was overwritten: %r" % (rows,))
    return {"head": case.oracle.head}


SCENARIOS = {
    # Workflow A: transaction writes vs. history reclamation
    "promise-blocks-reclamation": scenario_promise_blocks_reclamation,
    "multiple-readers-lowest-wins": scenario_multiple_readers_lowest_wins,
    "tombstone-and-history": scenario_tombstone_and_history,
    "tombstone-dropped": scenario_tombstone_dropped_when_horizon_passes,
    "horizon-cannot-regress": scenario_horizon_cannot_regress,
    "retained-counters": scenario_retained_counters_reflect_reality,
    "index-rebuilt-after-reclamation": scenario_index_rebuilt_after_reclamation,
    # Workflow B: closed transactions stop affecting others
    "rollback-leaves-no-trace": scenario_rollback_leaves_no_trace,
    "conflict-discards-all-writes": scenario_conflict_discards_all_writes,
    "conflict-after-vacuum": scenario_conflict_after_vacuum,
    "no-lost-update": scenario_no_lost_update_under_contention,
    "index-and-closed-transactions": scenario_index_and_closed_transactions,
    "private-writes-only-owner": scenario_private_writes_visible_only_to_owner,
    "commit-then-isolation": scenario_commit_then_snapshot_isolation,
    "collinear-head-still-audited": scenario_commit_does_not_hide_leaked_midstate,
    "closed-transactions-terminal": scenario_closed_transactions_are_terminal,
    # Workflow C: durability of both together
    "vacuum-survives-restart": scenario_vacuum_survives_restart,
    "checkpoint-and-vacuum": scenario_checkpoint_and_vacuum,
}

# Scenarios that need to start a second instance (backup/restore) take extra
# arguments; keeping them separate keeps the simple signatures uniform.
RESTORE_SCENARIOS = {
    "restore-preserves-horizon": scenario_restore_preserves_horizon_and_history,
    "restore-with-live-reader": scenario_restore_with_live_reader,
}


def run(name, binary, data_dir, verbose=False, temp_root=None):
    from .harness import Case

    case = Case(binary, data_dir, verbose=verbose)
    case.start()
    try:
        if name in RESTORE_SCENARIOS:
            if temp_root is None:
                raise VerificationFailure(
                    "scenario.needs-temp", "restore scenarios need a temp root")
            result = RESTORE_SCENARIOS[name](case, binary, temp_root)
        else:
            result = SCENARIOS[name](case)
        return {
            "status": "pass",
            "operations": case.operations,
            "audits": case.checks,
            "index_reads": case._index_selected,
            "head": case.oracle.head,
            "horizon": case.oracle.horizon,
            "result": result,
            "trace": case.observed,
        }
    finally:
        case.stop()


def all_names():
    return list(SCENARIOS) + list(RESTORE_SCENARIOS)
