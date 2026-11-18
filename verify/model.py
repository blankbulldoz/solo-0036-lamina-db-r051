"""Independent reference state machine for LaminaDB's two workflows.

The model is derived from the written contract only (PROJECT_SPEC.md,
README.md), never from the executor or the walkthrough's observations.
It tracks two distinct notions so that a wrong *physical* reclamation cannot
masquerade as a correct *logical* answer:

``history``
    Every committed version, in commit order, plus every vacuum horizon
    record.  This is the durable promise: for a snapshot ``at`` in the
    retained interval the answer must be the newest version with
    ``sequence <= at``.

``retained``
    The versions a correct vacuum may keep.  Used only to check space
    accounting facts (``GET /stats`` keys/versions, index entries, the
    removed/retained counters returned by ``POST /vacuum``).

The central invariant the task asks us to verify is exactly the bridge
between the two:

* a version still promised to an active transaction (snapshot >= horizon)
  must never be dropped from ``history``' readable set, and
* a closed transaction's private writes must never enter ``history`` at all.

Both are checked against the independent journal parse, so the answer does not
come from re-reading the executor's own responses.
"""

from __future__ import annotations

import json


class OracleError(AssertionError):
    """The observed history contradicts the documented state machine."""


INT_MIN = -(2 ** 63)
INT_MAX = 2 ** 63 - 1


def json_key(value):
    """Canonical encoding used for primary keys by the documented format."""
    if value is None:
        return "null:null"
    if isinstance(value, bool):
        return "bool:" + ("true" if value else "false")
    if isinstance(value, int):
        return "int:" + str(value)
    if isinstance(value, str):
        return "text:" + json.dumps(value, ensure_ascii=False)
    raise OracleError("unsupported scalar %r" % (value,))


def _freeze(value):
    """Hashable, type-preserving representation of a JSON scalar."""
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, int):
        return ("int", value)
    if isinstance(value, str):
        return ("text", value)
    return ("null", None)


def _index_pairs(chains, column):
    pairs = set()
    for (relation, key), chain in chains.items():
        for version in chain:
            if version["deleted"]:
                continue
            value = (version["row"] or {}).get(column)
            if value is None:
                continue
            pairs.add((_freeze(value), key))
    return len(pairs)


class History:
    """Pure replay of committed journal records into a queryable timeline.

    Sequences come from the journal, so this is independent evidence, not a
    copy of what the server reported at write time.
    """

    def __init__(self):
        self.sequences = []          # every applied record sequence in order
        self.commits = []            # {sequence, writes:[mutation...]}
        self.vacuums = []            # {sequence, horizon}
        self.versions = {}           # (relation, key) -> [ {sequence, row, deleted} ]
        self.physical = {}           # (relation, key) -> retained chain after vacuums
        self.relations = {}          # name -> created sequence
        self.horizon = 0
        self.head = 0

    # -- construction -----------------------------------------------------
    def apply(self, records):
        """Apply parsed journal records in sequence order."""
        for record in records:
            kind = record.get("kind")
            sequence = record["sequence"]
            self.sequences.append(sequence)
            if kind == "relation":
                relation = record["relation"]
                self.relations[relation["name"]] = relation["created"]
            elif kind == "index":
                pass
            elif kind == "commit":
                self._apply_commit(sequence, record.get("writes") or ())
            elif kind == "vacuum":
                # The durable record uses omitempty, so horizon 0 is absent.
                self._apply_vacuum(sequence, record.get("horizon", 0))
            else:
                raise OracleError("unknown journal record kind %r" % (kind,))
            self.head = max(self.head, sequence)
        return self

    def _apply_commit(self, sequence, writes):
        self.commits.append({"sequence": sequence, "writes": list(writes)})
        for mutation in writes:
            token = (mutation["relation"], mutation["key"])
            version = {
                "sequence": sequence,
                "row": None if mutation.get("deleted") else mutation.get("row"),
                "deleted": bool(mutation.get("deleted")),
            }
            self.versions.setdefault(token, []).append(version)
            self.physical.setdefault(token, []).append(version)

    def _apply_vacuum(self, sequence, effective_horizon):
        if effective_horizon < self.horizon:
            raise OracleError("vacuum horizon moved backwards at %d" % sequence)
        if effective_horizon > self.head:
            raise OracleError("vacuum horizon ahead of head at %d" % sequence)
        self.vacuums.append({"sequence": sequence, "horizon": effective_horizon})
        self.horizon = effective_horizon
        # Physical reclamation is monotone and irreversible.  Apply it to the
        # retained chains now; later commits append on top, so a key whose
        # tombstone was dropped cannot reappear in the space accounting.
        self.physical = self.retained_at(effective_horizon)

    # -- logical reads (the promise) --------------------------------------
    def visible_row(self, relation, key, at):
        chain = self.versions.get((relation, key), [])
        latest = None
        for version in chain:
            if version["sequence"] <= at:
                latest = version
            else:
                break
        if latest is None or latest["deleted"]:
            return None
        return latest["row"]

    def scan(self, relation, at):
        rows = {}
        for (name, key), chain in self.versions.items():
            if name != relation:
                continue
            row = self.visible_row(name, key, at)
            if row is not None:
                rows[key] = row
        return rows

    # -- retention model (what a correct vacuum may physically keep) -------
    def _prune(self, chains, at_horizon):
        """Chains a correct vacuum keeps when horizon becomes ``at_horizon``.

        Contract (README "变更与回收"): each chain keeps the last version at or
        before the horizon plus every later version; a final tombstone that is
        itself at or before the horizon removes the whole key.
        """
        kept = {}
        for token, chain in chains.items():
            if token[0].startswith("$"):
                continue
            if not chain:
                continue
            last = chain[-1]
            if last["sequence"] <= at_horizon and last["deleted"]:
                continue
            keep_index = 0
            for index, version in enumerate(chain):
                if version["sequence"] <= at_horizon:
                    keep_index = index
            kept[token] = chain[keep_index:]
        return kept

    def retained_at(self, at_horizon):
        """Physical chains a correct vacuum keeps at ``at_horizon``.

        Starts from the already-pruned physical chains, so repeated vacuums
        never resurrect a key that was previously dropped in full.
        """
        return self._prune(self.physical, at_horizon)

    def projected_retained_at(self, at_horizon):
        """What the physical chains *would* become at a larger horizon.

        Used before the engine performs a vacuum, to predict its counters.
        """
        return self._prune(self.physical, at_horizon)

    def last_commit_sequence(self, relation, key_token):
        """Newest committed sequence touching a key, from journal history.

        Used to justify first-committer-wins rejections independently of the
        executor's retained-chain state.
        """
        chain = self.versions.get((relation, key_token), [])
        return chain[-1]["sequence"] if chain else 0

    def expected_space(self, at_horizon):
        kept = self.retained_at(at_horizon)
        keys = len(kept)
        versions = sum(len(chain) for chain in kept.values())
        return keys, versions

    def projected_space(self, at_horizon):
        kept = self.projected_retained_at(at_horizon)
        keys = len(kept)
        versions = sum(len(chain) for chain in kept.values())
        return keys, versions

    def expected_index_entries(self, column):
        """Non-null (value, primary) candidate pairs a rebuilt index must hold.

        Contract (README "索引"): the index keeps historical non-null values
        mapped to primary keys, and is rebuilt after reclamation, so it holds
        exactly the candidate pairs derivable from the *retained* chains.
        """
        kept = self.retained_at(self.horizon)
        return _index_pairs(kept, column)

    def projected_index_entries(self, at_horizon, column):
        return _index_pairs(self.projected_retained_at(at_horizon), column)

    # -- the two invariants -------------------------------------------------
    def assert_reader_promise(self, relation, key, snapshot, expected_row):
        """A promised version must be readable by a still-active snapshot."""
        actual = self.visible_row(relation, key, snapshot)
        if actual != expected_row:
            raise OracleError(
                "reader promise broken for %s/%s at %d: history says %r, expected %r"
                % (relation, key, snapshot, actual, expected_row)
            )


def build(records):
    return History().apply(records)
