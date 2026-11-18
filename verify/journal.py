"""Independent parser for the LaminaDB write-ahead log.

This reads the durable log directly from its documented on-disk format:

    header : "LMW1" | base (uint64, big endian) | crc32(first 12 bytes)
    frame  : length (uint32 BE) | crc32(payload) (uint32 BE) | JSON payload

No part of the Go implementation is imported.  The parser exists so the
verification can *explain* the committed history from bytes on disk instead of
trusting the executor's own API responses or the demo script's observations.
"""

from __future__ import annotations

import json
import struct
import zlib
from dataclasses import dataclass


class JournalError(AssertionError):
    pass


@dataclass(frozen=True)
class JournalLog:
    base: int
    records: list
    tail_bytes: int

    @property
    def head(self) -> int:
        return self.records[-1]["sequence"] if self.records else self.base


def parse_file(path):
    with open(path, "rb") as handle:
        return parse_bytes(handle.read())


def parse_bytes(data):
    if len(data) < 16:
        raise JournalError("journal shorter than header")
    magic = data[:4]
    if magic != b"LMW1":
        raise JournalError("bad journal magic %r" % (magic,))
    base = struct.unpack(">Q", data[4:12])[0]
    if zlib.crc32(data[:12]) & 0xFFFFFFFF != struct.unpack(">I", data[12:16])[0]:
        raise JournalError("journal header checksum mismatch")

    records = []
    offset = 16
    last = base
    tail = 0
    while offset < len(data):
        remaining = len(data) - offset
        if remaining < 8:
            tail = remaining
            break
        length = struct.unpack(">I", data[offset:offset + 4])[0]
        checksum = struct.unpack(">I", data[offset + 4:offset + 8])[0]
        if length == 0 or length > 16 << 20:
            raise JournalError("invalid frame length %d at %d" % (length, offset))
        payload = data[offset + 8:offset + 8 + length]
        if len(payload) < length:
            tail = remaining
            break
        if zlib.crc32(payload) & 0xFFFFFFFF != checksum:
            raise JournalError("frame checksum mismatch at %d" % offset)
        try:
            record = json.loads(payload.decode("utf-8"))
        except ValueError as exc:
            raise JournalError("frame json at %d: %s" % (offset, exc)) from exc
        if record.get("format") != 1:
            raise JournalError("frame format at %d" % offset)
        if record.get("sequence") != last + 1:
            raise JournalError(
                "non-contiguous sequence at %d: %r after %d"
                % (offset, record.get("sequence"), last)
            )
        records.append(record)
        last = record["sequence"]
        offset += 8 + length
    return JournalLog(base=base, records=records, tail_bytes=tail)


def describe(log):
    """Human-readable commit history, independent of executor output."""
    lines = []
    for record in log.records:
        kind = record.get("kind")
        if kind == "commit":
            parts = []
            for mutation in record.get("writes") or ():
                if mutation.get("deleted"):
                    parts.append("%s/%s=DEL" % (mutation["relation"], mutation["key"]))
                else:
                    parts.append("%s/%s=%s" % (
                        mutation["relation"], mutation["key"],
                        json.dumps(mutation["row"], sort_keys=True, ensure_ascii=False),
                    ))
            lines.append("%d commit %s %s" % (record["sequence"], record.get("transaction"), " ".join(parts)))
        elif kind == "vacuum":
            lines.append("%d vacuum horizon=%d" % (record["sequence"], record["horizon"]))
        elif kind == "relation":
            lines.append("%d relation %s created=%s" % (
                record["sequence"], record["relation"]["name"], record["relation"]["created"]))
        elif kind == "index":
            lines.append("%d index %s" % (record["sequence"], record["index"]["name"]))
        else:
            lines.append("%d unknown %r" % (record["sequence"], kind))
    return "\n".join(lines)
