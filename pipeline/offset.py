"""Persistent replication-offset mirror.

The authoritative offset is the logical replication slot's confirmed_flush_lsn,
advanced via send_feedback() after every successful Mongo write — that is what lets
a restart resume without reprocessing already-applied events. We additionally mirror
the last-applied LSN into Mongo's _cdc_meta for observability (the lag metric reads
it) and as a human-inspectable sanity check.
"""
from pipeline.config import META_COLLECTION

_OFFSET_ID = "offset"


class OffsetStore:
    def __init__(self, db):
        self._col = db[META_COLLECTION]

    def save(self, lsn: int) -> None:
        """Record the last-applied LSN (called after each batch is durably written)."""
        self._col.update_one(
            {"_id": _OFFSET_ID},
            {"$set": {"lsn": lsn}},
            upsert=True,
        )

    def load(self) -> int:
        doc = self._col.find_one({"_id": _OFFSET_ID})
        return int(doc["lsn"]) if doc and doc.get("lsn") is not None else 0
