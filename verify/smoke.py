#!/usr/bin/env python3
"""Smoke-check the harness against a known-good baseline before exploring."""

import pathlib
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from verify import harness


def main():
    binary = pathlib.Path(__file__).resolve().parents[1] / "bin" / "laminad"
    with tempfile.TemporaryDirectory(prefix="lamina-smoke-") as temp:
        case = harness.Case(binary, pathlib.Path(temp) / "db", verbose=True)
        case.start()
        try:
            case.create_relation()
            a = case.begin()
            case.put(a, 1, 10, "alpha")
            case.put(a, 2, 20, "beta")
            case.commit(a)
            reader = case.begin(read_only=True)
            b = case.begin()
            case.put(b, 1, 99, "alpha2")
            case.commit(b)
            code, _ = case.server.put(reader, harness.REL, harness.make_row(3, 30, "gamma"))
            assert code == 409, "read-only put accepted: %s" % code
            code, _ = case.server.delete(reader, harness.REL, 2)
            assert code == 409, "read-only delete accepted: %s" % code
            case.vacuum(99)              # invalid: ahead of head
            case.vacuum(case.oracle.head)
            case.rollback(reader)
            case.vacuum(case.oracle.head)
            case.create_index("by_v", "v")
            case.checkpoint()
            case.restart()
            case.delete(case.begin(), 1)
            print("SMOKE OK; operations=%d checks=%d head=%d horizon=%d" % (
                case.operations, case.checks, case.oracle.head, case.oracle.horizon))
        finally:
            case.stop()


if __name__ == "__main__":
    main()
