# Independent state-machine verification

This directory verifies the two LaminaDB workflows the brief calls out:

1. **transaction writes** — a version promised to an active transaction is
   never reclaimed while that transaction can still read it; and
2. **history reclamation** — a closed transaction's private writes never
   affect anyone else, and reclamation respects the retained interval.

It is *independent*: it talks only to the documented HTTP surface, reads the
write-ahead log directly from its documented on-disk format, and rebuilds its
own model from `PROJECT_SPEC.md` / `README.md`. It does not import the Go
packages, and it does not copy expected values from
`examples/walkthrough.py`. The production sources are unchanged.

```
go build -o bin/laminad ./cmd/laminad
python3 verify/run.py --selftest      # the oracle's own evidence
python3 verify/run.py --all           # fixed, reviewable scenarios
python3 verify/run.py --explore --seed 1 --steps 200 --episodes 4
python3 verify/run.py --mutants       # prove the suite catches real faults
```

Every command exits non-zero on failure and prints a stable failure code.

The runner uses `bin/laminad` by default and refuses a binary it cannot
execute on the current platform (for example a macOS build checked out into a
Linux container), telling you to rebuild. Pass `--binary PATH` to point at a
different build, such as `bin/laminad-linux`.

## Why this is independent evidence

* `verify/journal.py` parses `journal.lmw` frame by frame (magic, base, CRC32,
  length, JSON, contiguous sequence) without the Go code. It reports
  incomplete tails separately from checksum corruption.
* `verify/model.py` replays those records into a timeline and defines
  visibility, retention and space accounting from the written contract. Its
  reads are answered from *committed history*, not from the engine's pruned
  chains, so a premature reclamation shows up as a missing promised row.
* `verify/selftest.py` checks the model against a second, deliberately naive
  implementation and against hand-computed cases, including the journal
  parser's corruption boundaries. A wrong oracle would make a passing run
  meaningless, so the oracle is tested too.
* Every expected value is derived from the journal and the spec. The
  engine's own `/stats`, `/changes`, `/query` responses are treated as
  observations to be checked, not as inputs to the expectations.

## What "same final row set hides a leak" means here

`scenarios.scenario_commit_does_not_hide_leaked_midstate` drives two updates
that return a row set to its original value while a reader stays open. Final
state alone cannot distinguish a correct engine from one that lost the
intermediate version. The harness therefore audits **after every operation**:
the whole retained interval `horizon..head`, every active transaction's
read-your-own-writes view, the change feed versus the raw journal, and the
space counters versus the retention model. The leak would be caught at the
step where the intermediate version disappears.

## Exploration strategy and its budget

Pure random schedules reach more state combinations but are hard to
reproduce and shrink; fixed schedules are readable but only cover what the
author imagined. The suite uses both:

* **Fixed scenarios** (`verify/scenarios.py`, 20 of them) isolate one state
  edge each and are meant to be read top to bottom.
* **Seeded exploration** (`verify/explore.py`) generates a schedule from a
  seed, records every operation, and replays it exactly. It is biased toward
  the two target workflows: overlapping live writers (to force
  first-committer-wins), read-only readers held open across vacuum (to force
  pinning), and periodic index/checkpoint/restart steps (to exercise the
  durable and physical state).

Reproducing and shrinking a failure is a three-liner:

```
python3 verify/run.py --explore --seed 7 --steps 200 --episodes 2 --save-trace /tmp/t.json
python3 verify/run.py --repro /tmp/t.json      # same failure, from the recorded schedule
python3 verify/run.py --shrink /tmp/t.json     # greedily drop steps, keeping the failure code
```

`--shrink` writes `/tmp/t-min.json`; a shrunk case is a short, replayable
schedule that still fails with the same code. Exploration is bounded by
`--steps` (per episode) and `--episodes`; there is no wall-clock or
unbounded loop.

## Proving the suite is not vacuous

`python3 verify/run.py --mutants` copies the sources into a scratch
directory, builds the unmodified engine plus seven deliberately broken
variants, and requires the fixed scenarios to fail on every one:

| mutant | broken invariant |
| --- | --- |
| `vacuum-ignores-active-readers` | active readers pin the vacuum horizon |
| `vacuum-reclaims-too-much` | the version at the horizon stays readable |
| `first-committer-wins-removed` | no lost updates under contention |
| `visible-uses-strict-less` | newest version with `sequence <= at` is visible |
| `rollback-unknown-succeeds` | closed transactions reject further work |
| `horizon-allows-regression` | the horizon cannot move backwards |
| `retained-count-short` | `removed`/`retained` counters reflect the store |

The unmodified build passes all fixed scenarios; every mutant is caught. The
mutation set is intentionally small and readable — it is evidence that the
checks bind, not a claim of exhaustive fault injection.

## What the scenarios cover

Grouped by the two workflows the brief names:

**Transaction writes promised to a reader are not reclaimed**
`promise-blocks-reclamation`, `multiple-readers-lowest-wins`,
`tombstone-and-history`, `tombstone-dropped`, `horizon-cannot-regress`,
`retained-counters`, `index-rebuilt-after-reclamation`.

**Closed transactions stop affecting others**
`rollback-leaves-no-trace`, `conflict-discards-all-writes`,
`conflict-after-vacuum`, `no-lost-update`,
`index-and-closed-transactions`, `private-writes-only-owner`,
`commit-then-isolation`, `collinear-head-still-audited`,
`closed-transactions-terminal`.

**Both, across durability boundaries**
`vacuum-survives-restart`, `checkpoint-and-vacuum`,
`restore-preserves-horizon`, `restore-with-live-reader`.

For each scenario the harness checks, at each step: head/horizon/history base
match the journal; every retained snapshot (`at`) matches the oracle through
both scan and index paths; active transactions read exactly their snapshot
plus private writes; the change feed equals the raw journal; `keys`/`versions`
and index entries match the retention model; and closed transactions reject
every follow-up call with 404.

## Boundaries this does not cover

Stated plainly so the evidence is not over-read:

* **Concurrency is scheduled, not stressed.** All interleavings are serialized
  by the harness's own sequencing; the engine still takes a global lock. The
  suite does not run parallel clients against the mutex, so it says nothing
  about lock ordering, fairness or true multi-threaded races.
* **Crash recovery is a clean restart, not a crash.** `restart` uses a
  graceful SIGTERM. There is no test that kills mid-`write`/mid-`fsync` and
  inspects a partially durable frame; the journal parser can *read* such a
  state but the suite does not produce one.
* **No power-loss or disk-fault injection.** Storage-uncertainty handling
  (`503` + poison) is documented but not exercised.
* **Budgets and limits are not load-tested.** Capacity (`128` transactions,
  `1000` writes, `1MiB` bodies, `10k` result rows), request timeouts and the
  64KiB text limit are not driven to their edges.
* **SQL, aggregation and the CLI decoders** are only used incidentally (as
  alternate read paths) and are not the subject of dedicated oracle checks.
* **Exploration is bounded by the step budget.** Longer schedules, more
  concurrent sessions, or a different seed may reach states these runs did
  not. The recorded-schedule workflow exists so any new failure reported by
  someone else can be replayed and shrunk here.
* **The model assumes the documented contract.** If the spec and the code
  disagreed about, for example, whether an empty commit consumes a sequence,
  the oracle would encode the documented behaviour; the current engine
  returns the transaction snapshot and allocates nothing, which the harness
  checks explicitly.

## Files

| file | role |
| --- | --- |
| `journal.py` | independent WAL parser (header, CRC32 frames, tail) |
| `model.py` | reference state machine: visibility, retention, space, index entries |
| `server.py` | HTTP client plus process control |
| `harness.py` | drives operations and audits every step against the oracle |
| `scenarios.py` | 20 fixed, reviewable interleavings |
| `explore.py` | seeded schedule generation, replay, shrinking |
| `selftest.py` | differential and hand-computed checks for the oracle itself |
| `mutants.py` | builds broken engines to prove the suite catches faults |
| `run.py` | CLI entry point |
| `smoke.py` | quick harness sanity check |
