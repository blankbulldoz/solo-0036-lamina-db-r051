#!/usr/bin/env python3
"""Entry point for LaminaDB's independent state-machine verification.

Usage::

    python3 verify/run.py --list
    python3 verify/run.py --scenario promise-blocks-reclamation
    python3 verify/run.py --all
    python3 verify/run.py --explore --seed 7 --steps 120
    python3 verify/run.py --repro failed.json
    python3 verify/run.py --selftest

The suite drives only the documented HTTP surface and reads the durable
journal independently.  It does not import the Go code and does not reuse
examples/walkthrough.py observations as expected values.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

from verify import explore, mutants, scenarios  # noqa: E402
from verify.harness import VerificationFailure  # noqa: E402


def default_binary(args):
    if args.binary:
        binary = pathlib.Path(args.binary).resolve()
        if not binary.exists():
            raise SystemExit("binary not found: %s" % binary)
        _check_native(binary)
        return binary
    candidate = ROOT / "bin" / "laminad"
    if not candidate.exists():
        raise SystemExit("build first: go build -o bin/laminad ./cmd/laminad")
    _check_native(candidate)
    return candidate


def _check_native(binary):
    """Fail with a clear message if the binary cannot run on this machine."""
    try:
        probe = subprocess.run([str(binary), "-h"], capture_output=True, timeout=5)
    except OSError as exc:
        raise SystemExit(
            "%s cannot run on this platform (%s); "
            "rebuild with: go build -o %s ./cmd/laminad"
            % (binary, exc, binary))
    except subprocess.TimeoutExpired:
        return
    stderr = (probe.stderr or b"").decode(errors="replace")
    if probe.returncode not in (0, 2) or "Exec format error" in stderr:
        raise SystemExit(
            "%s cannot run on this platform (rc=%d: %s); "
            "rebuild with: go build -o %s ./cmd/laminad"
            % (binary, probe.returncode, stderr.strip() or "unknown", binary))


def run_scenarios(binary, names, verbose=False, keep=False):
    outcomes = []
    for name in names:
        holder = tempfile.mkdtemp(prefix="lamina-verify-")
        try:
            result = scenarios.run(
                name, binary, pathlib.Path(holder) / "db",
                verbose=verbose, temp_root=pathlib.Path(holder))
            outcomes.append({"kind": "scenario", "name": name, **result})
            print("PASS %-34s ops=%-3d audits=%-3d index-reads=%-3d head=%s horizon=%s" % (
                name, result["operations"], result["audits"],
                result.get("index_reads", 0), result["head"], result["horizon"]))
        except VerificationFailure as exc:
            outcomes.append({
                "kind": "scenario", "name": name, "status": "fail",
                "code": exc.code, "detail": exc.detail, "step": exc.step,
                "context": exc.context,
            })
            print("FAIL %-34s %s" % (name, exc))
        finally:
            if not keep:
                shutil.rmtree(holder, ignore_errors=True)
            else:
                print("  kept data dir:", pathlib.Path(holder) / "db")
    return outcomes


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", help="path to the laminad binary")
    parser.add_argument("--list", action="store_true", help="list scenarios and exit")
    parser.add_argument("--scenario", action="append", default=[], metavar="NAME")
    parser.add_argument("--all", action="store_true", help="run every fixed scenario")
    parser.add_argument("--explore", action="store_true", help="run seeded random exploration")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--steps", type=int, default=120)
    parser.add_argument("--episodes", type=int, default=1)
    parser.add_argument("--save-trace", metavar="PATH", help="write the exploration schedule")
    parser.add_argument("--repro", metavar="PATH", help="re-run a saved failing schedule")
    parser.add_argument("--shrink", metavar="PATH", help="minimise a saved failing schedule")
    parser.add_argument("--selftest", action="store_true",
                        help="run the model's own differential checks")
    parser.add_argument("--mutants", action="store_true",
                        help="build broken engines in a scratch dir and prove the suite catches them")
    parser.add_argument("--json", action="store_true", help="machine-readable outcome")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--keep", action="store_true", help="keep temporary data directories")
    args = parser.parse_args(argv)

    if args.list:
        for name in scenarios.all_names():
            print(name)
        return 0

    binary = default_binary(args)

    if args.selftest:
        from verify import selftest as selftest_module
        return selftest_module.main(binary, verbose=args.verbose, as_json=args.json)

    if args.mutants:
        report = mutants.run(ROOT, verbose=not args.json or args.verbose)
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            caught = sum(1 for r in report["results"] if r.get("caught"))
            total = len(report["results"])
            print("mutants: %d/%d caught" % (caught, total))
            for result in report["results"]:
                status = "caught" if result.get("caught") else (
                    "error" if result.get("caught") is None else "MISSED")
                print("  %-34s %-8s %s" % (
                    result["mutant"], status, result.get("invariant", "")))
        return 0 if all(r.get("caught") for r in report["results"]) else 1

    if args.repro:
        outcome = explore.repro(pathlib.Path(args.repro), binary, verbose=args.verbose)
        print(json.dumps(outcome, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if outcome.get("status") == "reproduced" else 1

    if args.shrink:
        source = pathlib.Path(args.shrink)
        outcome = explore.shrink(source, binary, verbose=args.verbose)
        if outcome.get("status") == "shrunk":
            destination = source.with_name(source.stem + "-min.json")
            destination.write_text(
                json.dumps(outcome["schedule"], indent=2, ensure_ascii=False),
                encoding="utf-8")
            print("shrunk to %d operations -> %s (failure code %s)" % (
                len(outcome["schedule"]["operations"]), destination, outcome["code"]))
        else:
            print(json.dumps(outcome, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if outcome.get("status") == "shrunk" else 1

    if args.explore:
        report = explore.explore(
            binary, seed=args.seed, steps=args.steps, episodes=args.episodes,
            verbose=args.verbose, save_trace=args.save_trace)
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print("explore seed=%d episodes=%d steps<=%d" % (
                args.seed, args.episodes, args.steps))
            print("  passing episodes: %d" % report["passing"])
            print("  failing episodes: %d" % len(report["failures"]))
            for failure in report["failures"]:
                print("  FAIL %s" % failure["message"])
            print("  coverage: %s" % json.dumps(report["coverage"], sort_keys=True))
        return 0 if not report["failures"] else 1

    names = scenarios.all_names() if args.all else args.scenario
    if not names:
        parser.error("choose --all, --scenario NAME, --explore, --repro or --selftest")
    outcomes = run_scenarios(binary, names, verbose=args.verbose, keep=args.keep)
    if args.json:
        print(json.dumps(outcomes, ensure_ascii=False, indent=2, sort_keys=True))
    failed = [o for o in outcomes if o["status"] != "pass"]
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
