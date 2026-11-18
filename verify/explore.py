"""Seeded, replayable exploration over the transaction/history state machine.

Why not pure random or pure fixed?

* Pure random exploration reaches far more state combinations, but a failure
  deep in a long schedule is painful to reproduce and shrink.
* Pure fixed scenarios are trivially reviewable but only cover the states the
  author thought of.

This module takes the middle path the brief asks for: a *seeded* generator
that records every chosen operation into a portable schedule file.  A failure
is therefore fully reproducible from ``(seed, steps, episode)`` *and* from the
recorded schedule, and shrinking works on the schedule, not on random state.
Exploration is bounded by an explicit step budget so runs are reviewable.

The generator is biased toward the two target workflows rather than being
uniform: it deliberately keeps overlapping transactions alive across
commits (to force first-committer-wins), holds read-only readers open while
vacuum is requested (to force pinning), and periodically adds an index,
checkpoint or restart so both physical and durable state get exercised.
"""

from __future__ import annotations

import json
import pathlib
import random
import shutil
import tempfile

from .harness import Case, VerificationFailure

VALUES = (0, 5, 10, 20, 40)
TEXTS = ("alpha", "beta", "gamma", None)
KEYS = (1, 2, 3, 4, 5, 6)
STEPS_OPS = ("begin", "put", "delete", "commit", "rollback", "vacuum",
             "index", "checkpoint", "restart")


class Schedule:
    """Recorded operation log that can be replayed exactly."""

    def __init__(self, seed, steps):
        self.seed = seed
        self.steps_requested = steps
        self.operations = []

    def record(self, op):
        self.operations.append(op)
        return op

    def to_json(self):
        return {
            "format": 1,
            "seed": self.seed,
            "steps_requested": self.steps_requested,
            "operations": self.operations,
        }

    @classmethod
    def from_json(cls, payload):
        schedule = cls(payload["seed"], payload["steps_requested"])
        schedule.operations = list(payload["operations"])
        return schedule


def _generate(seed, steps):
    """Produce a deterministic operation schedule from a seed."""
    rng = random.Random(seed)
    schedule = Schedule(seed, steps)
    open_slots = []
    counter = {"n": 0}

    def new_slot(read_only=False):
        name = "t%d" % counter["n"]
        counter["n"] += 1
        schedule.record({"op": "begin", "slot": name, "read_only": read_only})
        open_slots.append((name, read_only))
        return name

    writable = new_slot()
    while len(schedule.operations) < steps:
        choice = rng.random()
        writable_slots = [n for n, ro in open_slots if not ro]
        readers = [n for n, ro in open_slots if ro]

        if choice < 0.12:
            # Prefer opening a second overlapping writer to create conflicts.
            new_slot(read_only=(rng.random() < 0.22))
        elif choice < 0.50:
            target = rng.choice(writable_slots) if writable_slots else new_slot()
            schedule.record({
                "op": "put", "slot": target, "key": rng.choice(KEYS),
                "value": rng.choice(VALUES), "text": rng.choice(TEXTS),
                "insert_only": rng.random() < 0.10,
            })
        elif choice < 0.58:
            target = rng.choice(writable_slots) if writable_slots else new_slot()
            schedule.record({"op": "delete", "slot": target,
                             "key": rng.choice(KEYS)})
        elif choice < 0.74:
            # Commit an *older* writer while a newer one stays open: this is
            # what turns an overlapping write into a real 409.
            if writable_slots:
                target = rng.choice(writable_slots)
                open_slots.remove((target, False))
                schedule.record({"op": "commit", "slot": target})
            else:
                new_slot()
        elif choice < 0.81:
            if writable_slots:
                target = rng.choice(writable_slots)
                open_slots.remove((target, False))
                schedule.record({"op": "rollback", "slot": target})
            else:
                new_slot()
        elif choice < 0.89:
            # Vacuum while readers are (usually) still open.
            schedule.record({"op": "vacuum", "requested": rng.choice(
                ("head", "horizon", "mid", "head", "mid"))})
        elif choice < 0.93:
            index = "ix%d" % len([o for o in schedule.operations if o["op"] == "index"])
            schedule.record({"op": "index", "name": index,
                             "column": rng.choice(("v", "t")),
                             "unique": rng.random() < 0.25})
        elif choice < 0.97:
            schedule.record({"op": "checkpoint"})
        else:
            # Restart with live transactions too: they must vanish without
            # leaving durable traces, and the job is to prove the audit still
            # holds for whatever was actually committed.
            schedule.record({"op": "restart"})
            open_slots.clear()
        if not open_slots:
            new_slot()
    return schedule


def _resolve_vacuum(operation, case, index, seed):
    requested = operation.get("requested")
    if requested == "head":
        return case.oracle.head
    if requested == "horizon":
        return case.oracle.horizon
    if requested == "mid":
        span = case.oracle.head - case.oracle.horizon
        return case.oracle.horizon + span // 2
    if requested is None:
        return random.Random(seed + index).choice(
            (case.oracle.head, case.oracle.horizon))
    return requested


def _play(schedule, binary, data_dir, verbose=False):
    """Replay a schedule against a fresh instance, auditing every step."""
    case = Case(binary, data_dir, verbose=verbose)
    case.start()
    slots = {}
    stats = {"commits": 0, "conflicts": 0, "rollbacks": 0, "deletes": 0,
             "vacums_with_reader": 0, "pinned_vacuums": 0,
             "insert_only_rejections": 0, "reclaims": 0,
             "indexes": 0, "checkpoints": 0, "restarts": 0}
    try:
        case.create_relation()
        for index, operation in enumerate(schedule.operations):
            kind = operation["op"]
            if kind == "begin":
                slot = operation["slot"]
                if slot in slots:
                    raise VerificationFailure(
                        "schedule.reuse-slot", "slot %s already open" % (slot,))
                slots[slot] = case.begin(read_only=operation.get("read_only", False))
            elif kind == "put":
                tx = slots[operation["slot"]]
                code = case.put(tx, operation["key"], operation["value"],
                                operation.get("text"),
                                insert_only=operation.get("insert_only", False))
                if code == 409:
                    stats["insert_only_rejections"] += 1
            elif kind == "delete":
                case.delete(slots[operation["slot"]], operation["key"])
                stats["deletes"] += 1
            elif kind == "commit":
                code, payload = case.commit(slots.pop(operation["slot"]))
                if code == 200:
                    stats["commits"] += 1
                elif code == 409:
                    stats["conflicts"] += 1
            elif kind == "rollback":
                case.rollback(slots.pop(operation["slot"]))
                stats["rollbacks"] += 1
            elif kind == "vacuum":
                readers_open = any(info["read_only"] for info in case.active.values())
                before_horizon = case.oracle.horizon
                code, payload = case.vacuum(_resolve_vacuum(operation, case, index, schedule.seed))
                if code == 200:
                    if readers_open:
                        stats["vacums_with_reader"] += 1
                    if payload.get("pinned"):
                        stats["pinned_vacuums"] += 1
                    if payload.get("horizon", before_horizon) > before_horizon:
                        stats["reclaims"] += 1
            elif kind == "index":
                try:
                    case.create_index(operation["name"], operation["column"],
                                      unique=operation.get("unique", False))
                    stats["indexes"] += 1
                except VerificationFailure as exc:
                    if exc.code != "index.create":
                        raise
                    # A rejected unique build is a legitimate documented failure;
                    # the audit after index.create still runs only on success, so
                    # verify the catalog stayed unchanged here.
                    if any(i["name"] == operation["name"]
                           for i in case.server.catalog()["indexes"]):
                        raise VerificationFailure(
                            "index.rejected-but-published",
                            "failed unique build left a catalog entry")
            elif kind == "checkpoint":
                case.checkpoint()
                stats["checkpoints"] += 1
            elif kind == "restart":
                case.restart()
                slots.clear()
                stats["restarts"] += 1
            else:
                raise VerificationFailure("schedule.unknown-op", "op %r" % (kind,))
        for slot, tx in list(slots.items()):
            case.rollback(tx)
        slots.clear()
        return {
            "status": "pass",
            "steps": len(schedule.operations),
            "audits": case.checks,
            "head": case.oracle.head,
            "horizon": case.oracle.horizon,
            "coverage": stats,
        }
    finally:
        case.stop()


def explore(binary, seed, steps, episodes=1, verbose=False, save_trace=None):
    """Run one or more bounded seeded episodes and summarise coverage."""
    totals = {key: 0 for key in (
        "commits", "conflicts", "rollbacks", "deletes", "vacums_with_reader",
        "pinned_vacuums", "insert_only_rejections", "reclaims", "indexes",
        "checkpoints", "restarts")}
    failures = []
    passing = 0
    for episode in range(episodes):
        episode_seed = seed + episode * 100003
        schedule = _generate(episode_seed, steps)
        holder = tempfile.mkdtemp(prefix="lamina-explore-")
        try:
            outcome = _play(schedule, binary, pathlib.Path(holder) / "db", verbose=verbose)
            passing += 1
            for key, value in outcome["coverage"].items():
                totals[key] += value
        except VerificationFailure as exc:
            failures.append({
                "episode": episode,
                "seed": episode_seed,
                "message": str(exc),
                "code": exc.code,
                "step": exc.step,
                "context": exc.context,
                "schedule": schedule.to_json(),
            })
        finally:
            shutil.rmtree(holder, ignore_errors=True)
        # Each episode's schedule is written regardless of outcome, so any
        # failure (and any interesting pass) can be replayed without the seed.
        if save_trace:
            path = pathlib.Path(save_trace)
            if episodes > 1:
                path = path.with_name("%s-ep%d.json" % (path.stem, episode))
            path.write_text(json.dumps(schedule.to_json(), indent=2), encoding="utf-8")
    return {
        "seed": seed,
        "episodes": episodes,
        "steps": steps,
        "passing": passing,
        "failures": failures,
        "coverage": totals,
    }


def repro(path, binary, verbose=False):
    """Replay a saved schedule and report whether it reproduces."""
    payload = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    schedule = Schedule.from_json(payload)
    holder = tempfile.mkdtemp(prefix="lamina-repro-")
    try:
        try:
            outcome = _play(schedule, binary, pathlib.Path(holder) / "db", verbose=verbose)
        except VerificationFailure as exc:
            return {
                "status": "reproduced",
                "code": exc.code,
                "detail": exc.detail,
                "step": exc.step,
                "context": exc.context,
            }
        return {"status": "did-not-reproduce", "outcome": outcome}
    finally:
        shutil.rmtree(holder, ignore_errors=True)


def shrink(path, binary, verbose=False):
    """Greedily drop operations while the same failure code still reproduces.

    Returns a minimal schedule that preserves the first failing operation's
    error code.  Shrinking is deterministic and operates on the recorded
    schedule, so a shrunk case remains reviewable and replayable.
    """
    payload = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    schedule = Schedule.from_json(payload)
    target = None
    holder = tempfile.mkdtemp(prefix="lamina-shrink-")
    try:
        try:
            _play(schedule, binary, pathlib.Path(holder) / "db", verbose=verbose)
            return {"status": "did-not-reproduce"}
        except VerificationFailure as exc:
            target = exc.code
        operations = list(schedule.operations)
        changed = True
        while changed:
            changed = False
            candidate_index = 0
            while candidate_index < len(operations):
                trial = Schedule(schedule.seed, schedule.steps_requested)
                trial.operations = operations[:candidate_index] + operations[candidate_index + 1:]
                shutil.rmtree(holder, ignore_errors=True)
                holder = tempfile.mkdtemp(prefix="lamina-shrink-")
                try:
                    _play(trial, binary, pathlib.Path(holder) / "db", verbose=False)
                except VerificationFailure as exc:
                    if exc.code == target:
                        operations = trial.operations
                        changed = True
                        continue
                except Exception:
                    pass
                candidate_index += 1
        schedule.operations = operations
        return {"status": "shrunk", "code": target, "schedule": schedule.to_json()}
    finally:
        shutil.rmtree(holder, ignore_errors=True)
