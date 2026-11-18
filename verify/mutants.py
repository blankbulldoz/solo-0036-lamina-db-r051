"""Mutation testing: prove the verification is not vacuous.

A verification suite that passes on a correct engine proves little unless it
*also fails* on an incorrect one.  This module builds deliberately broken
copies of LaminaDB in a scratch directory (never touching the working tree)
and checks that the fixed scenarios catch each fault.

Each fault corresponds to a documented invariant:

* ``vacuum-ignores-active-readers``  -- "active transactions retain versions"
* ``vacuum-reclaims-too-much``       -- "the lowest active read timestamp limits pruning"
* ``first-committer-wins-removed``   -- "first committer wins" / no lost updates
* ``visible-uses-strict-less``       -- "the newest version with sequence <= at"
* ``rollback-unknown-succeeds``      -- "closed transactions are not reusable"
* ``horizon-allows-regression``      -- "the horizon cannot go backwards"
* ``retained-count-short``           -- vacuum removed/retained counters are facts

The mutation set is intentionally small and readable; it is evidence that the
checks bind, not a claim of exhaustive fault injection.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import tempfile

MUTATIONS = {
    "vacuum-ignores-active-readers": {
        "file": "internal/engine/maintenance.go",
        "old": ("\tfor _, tx := range d.transactions {\n"
                "\t\tif tx.Snapshot < horizon {\n"
                "\t\t\thorizon = tx.Snapshot\n"
                "\t\t}\n"
                "\t}\n"),
        "new": "",
        "invariant": "active readers pin the vacuum horizon",
    },
    "vacuum-reclaims-too-much": {
        "file": "internal/versions/vacuum.go",
        "old": ("for keep+1 < len(chain) && chain[keep+1].Sequence <= horizon {\n"
                "\t\t\t\tkeep++\n"
                "\t\t\t}"),
        "new": ("for keep+1 < len(chain) && chain[keep+1].Sequence < horizon {\n"
                "\t\t\t\tkeep++\n"
                "\t\t\t}"),
        "invariant": "horizon boundary retains the version at the horizon",
    },
    "first-committer-wins-removed": {
        "file": "internal/engine/transaction.go",
        "old": "if d.store.Latest(table, key) > tx.Snapshot {",
        "new": "if false && d.store.Latest(table, key) > tx.Snapshot {",
        "invariant": "first committer wins; no lost updates",
    },
    "visible-uses-strict-less": {
        "file": "internal/versions/store.go",
        "old": "if chain[mid].Sequence <= at {",
        "new": "if chain[mid].Sequence < at {",
        "invariant": "newest version with sequence <= at is visible",
    },
    "rollback-unknown-succeeds": {
        "file": "internal/engine/transaction.go",
        "old": ("\tif _, err := d.tx(id); err != nil {\n"
                "\t\treturn err\n"
                "\t}\n"
                "\tdelete(d.transactions, id)\n"
                "\treturn nil\n"
                "}"),
        "new": "\tdelete(d.transactions, id)\n\treturn nil\n}",
        "invariant": "closed transactions reject further operations",
    },
    "horizon-allows-regression": {
        "file": "internal/engine/maintenance.go",
        "old": "if requested > d.sequence || requested < d.horizon {",
        "new": "if requested > d.sequence {",
        "invariant": "vacuum horizon cannot move backwards",
    },
    "retained-count-short": {
        "file": "internal/engine/maintenance.go",
        "old": ("return versions.VacuumResult{Horizon: horizon, "
                "Removed: beforeVersions - remaining, Retained: remaining, "
                "Pinned: horizon < requested}, nil"),
        "new": ("return versions.VacuumResult{Horizon: horizon, "
                "Removed: beforeVersions - remaining + 1, Retained: remaining, "
                "Pinned: horizon < requested}, nil"),
        "invariant": "removed/retained counters reflect the store change",
    },
}


def _find_go():
    override = os.environ.get("GO")
    if override:
        return override
    found = shutil.which("go")
    if found:
        return found
    for candidate in ("/tmp/gotool/go/bin/go", "/usr/local/go/bin/go"):
        if pathlib.Path(candidate).exists():
            return candidate
    raise SystemExit("go toolchain not found; set GO=/path/to/go")


def _copy_tree(root, destination):
    for name in ("internal", "cmd", "go.mod"):
        source = root / name
        target = destination / name
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            shutil.copy2(source, target)


def _build(source_root, go, cache, output):
    env = dict(os.environ, GOCACHE=str(cache), CGO_ENABLED="0")
    result = subprocess.run(
        [go, "build", "-o", str(output), "./cmd/laminad"],
        cwd=str(source_root), env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError("build failed:\n" + result.stderr)


def run(root, verbose=False):
    """Build the baseline and every mutant; return a machine-readable report."""
    root = pathlib.Path(root).resolve()
    go = _find_go()
    work = pathlib.Path(tempfile.mkdtemp(prefix="lamina-mutants-"))
    cache = work / "gocache"
    cache.mkdir()
    results = []
    try:
        baseline_src = work / "baseline"
        baseline_src.mkdir()
        _copy_tree(root, baseline_src)
        baseline_bin = work / "laminad-baseline"
        _build(baseline_src, go, cache, baseline_bin)

        from . import scenarios
        baseline_failures = _run_fixed(baseline_bin, scenarios)
        results.append({
            "mutant": "baseline",
            "invariant": "unmodified engine",
            "caught": not baseline_failures,
            "failures": baseline_failures,
        })
        if verbose:
            print("baseline: %s" % ("pass" if not baseline_failures else "FAILED"))

        for name, spec in MUTATIONS.items():
            mutant_src = work / name
            mutant_src.mkdir()
            _copy_tree(root, mutant_src)
            target = mutant_src / spec["file"]
            text = target.read_text(encoding="utf-8")
            count = text.count(spec["old"])
            if count != 1:
                results.append({
                    "mutant": name, "invariant": spec["invariant"],
                    "caught": None,
                    "error": "mutation site matched %d times" % count,
                })
                continue
            target.write_text(text.replace(spec["old"], spec["new"], 1), encoding="utf-8")
            mutant_bin = work / ("laminad-" + name)
            try:
                _build(mutant_src, go, cache, mutant_bin)
            except RuntimeError as exc:
                results.append({
                    "mutant": name, "invariant": spec["invariant"],
                    "caught": None, "error": str(exc),
                })
                continue
            failures = _run_fixed(mutant_bin, scenarios)
            caught = bool(failures)
            results.append({
                "mutant": name,
                "invariant": spec["invariant"],
                "caught": caught,
                "failures": failures,
            })
            if verbose:
                print("%-32s %s (%d scenario failures)" % (
                    name, "caught" if caught else "MISSED", len(failures)))
        return {"results": results}
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _run_fixed(binary, scenarios):
    """Run every fixed scenario against ``binary``; return failing names."""
    import tempfile as tf
    failures = []
    for name in scenarios.all_names():
        holder = tf.mkdtemp(prefix="lamina-mutant-run-")
        try:
            scenarios.run(name, binary, pathlib.Path(holder) / "db",
                           temp_root=pathlib.Path(holder))
        except Exception as exc:  # a crash is also a catch
            failures.append({"scenario": name, "error": str(exc)[:300],
                             "code": getattr(exc, "code", "crash")})
        finally:
            shutil.rmtree(holder, ignore_errors=True)
    return failures
