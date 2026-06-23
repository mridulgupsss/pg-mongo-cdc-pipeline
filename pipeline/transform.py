"""Pure transformation layer: maps a decoded change (from wal2json OR from the
snapshot reader) into a MongoDB write operation. No I/O happens here, so every
function below is unit-testable in isolation.

Two callers share this module so that the snapshot path and the streaming path
produce byte-identical documents (required for field-parity test T5):
  - reader/writer  -> change_to_op()
  - snapshot       -> build_document()

Canonical type coercion (applied by BOTH paths so PG and Mongo agree):
  numeric/decimal     -> bson Decimal128 (exact, no float rounding)
  timestamp/date      -> ISO-8601 UTC string (microsecond precision preserved)
  boolean             -> bool
  integer/bigint      -> int
  text/enum/other     -> str
"""
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bson.decimal128 import Decimal128


# ---------------------------------------------------------------------------
# Value coercion
# ---------------------------------------------------------------------------

def _parse_timestamp(value: Any) -> datetime:
    """Parse a Python datetime (snapshot path) or a wal2json timestamp string
    (stream path) into a tz-aware datetime. Robust across Python versions: wal2json
    emits offsets like "+00", which datetime.fromisoformat rejects before 3.11."""
    if isinstance(value, datetime):
        return value
    s = str(value).strip().replace(" ", "T")
    s = re.sub(r"Z$", "+00:00", s)
    s = re.sub(r"([+-]\d{2})$", r"\1:00", s)        # "+00" -> "+00:00"
    s = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", s)  # "+0000" -> "+00:00"
    return datetime.fromisoformat(s)


def canonical_timestamp(value: Any) -> str:
    """Return a single canonical ISO-8601 UTC string for a timestamp value."""
    dt = _parse_timestamp(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def coerce_value(pg_type: str, value: Any) -> Any:
    """Coerce a single column value to its canonical storage form, given the
    PostgreSQL type name. Accepts both native Python values (snapshot) and the
    string/JSON values wal2json produces (stream)."""
    if value is None:
        return None
    t = pg_type.lower()
    if "numeric" in t or "decimal" in t or "money" in t:
        return Decimal128(str(value))
    if "timestamp" in t or t == "date":
        return canonical_timestamp(value)
    if t in ("boolean", "bool"):
        return value if isinstance(value, bool) else str(value).lower() in ("t", "true", "1")
    if t in ("smallint", "integer", "int", "bigint", "int2", "int4", "int8"):
        return int(value)
    return str(value)


# ---------------------------------------------------------------------------
# wal2json column helpers
# ---------------------------------------------------------------------------

def columns_to_dict(columns: List[dict]) -> Dict[str, Any]:
    """wal2json columns/identity array -> {name: raw_value}."""
    return {c["name"]: c.get("value") for c in columns}


def columns_to_types(columns: List[dict]) -> Dict[str, str]:
    """wal2json columns/identity array -> {name: pg_type} (needs include-types)."""
    return {c["name"]: c.get("type", "text") for c in columns}


def diff_changed(new: Dict[str, Any], old: Dict[str, Any]) -> Dict[str, Any]:
    """Columns whose value actually changed -> enables partial $set on UPDATE
    instead of a full document replacement (spec 4.2)."""
    return {k: v for k, v in new.items() if k not in old or old[k] != v}


# ---------------------------------------------------------------------------
# Operation model
# ---------------------------------------------------------------------------

@dataclass
class MongoOp:
    """A pending MongoDB write, keyed by the PG primary key (= Mongo _id)."""
    table: str
    pk_value: Any
    set_fields: Dict[str, Any]   # canonicalised field values, incl. _lsn (+ deleted_at on delete)
    lsn: int = 0                 # scalar LSN for the monotonic write guard
    is_delete: bool = False


def build_set_fields(values: Dict[str, Any], types: Dict[str, str],
                     tracked: set, lsn: int) -> Dict[str, Any]:
    """Coerce a row's columns to canonical form, keeping only tracked columns.
    Untracked (newly-added, un-synced) columns are skipped here — the caller logs
    the drift warning. Always stamps _lsn for the idempotency/ordering guard."""
    doc = {col: coerce_value(types.get(col, "text"), val)
           for col, val in values.items() if col in tracked}
    doc["_lsn"] = lsn
    return doc


def change_to_op(change: dict, pk_col: str, tracked: set, lsn: int) -> Optional[MongoOp]:
    """Map one wal2json format-v2 change object to a MongoOp, or None for
    non-DML messages (begin/commit/etc.)."""
    action = change.get("action")
    table = change.get("table")

    if action == "I":
        values = columns_to_dict(change["columns"])
        types = columns_to_types(change["columns"])
        fields = build_set_fields(values, types, tracked, lsn)
        return MongoOp(table, values[pk_col], fields, lsn=lsn, is_delete=False)

    if action == "U":
        new_vals = columns_to_dict(change["columns"])
        new_types = columns_to_types(change["columns"])
        old_vals = columns_to_dict(change.get("identity", []))
        changed = diff_changed(new_vals, old_vals)
        fields = build_set_fields(changed, new_types, tracked, lsn)
        return MongoOp(table, new_vals[pk_col], fields, lsn=lsn, is_delete=False)

    if action == "D":
        old_vals = columns_to_dict(change.get("identity", []))
        deleted_at = canonical_timestamp(change["timestamp"]) if change.get("timestamp") \
            else datetime.now(timezone.utc).isoformat()
        return MongoOp(table, old_vals[pk_col],
                       {"deleted_at": deleted_at, "_lsn": lsn}, lsn=lsn, is_delete=True)

    # B (begin), C (commit), M (message), etc. -> nothing to write.
    return None


# ---------------------------------------------------------------------------
# Mongo update body (LSN-guarded so stale replays never regress a newer value)
# ---------------------------------------------------------------------------

def build_pipeline_update(set_fields: Dict[str, Any], lsn: int) -> List[dict]:
    """Aggregation-pipeline update that conditionally applies each field only when
    the incoming LSN is newer than the stored _lsn. Works with upsert (missing
    _lsn is treated as -1), giving idempotent, monotonic, out-of-order-safe writes."""
    guard = {"$gt": [lsn, {"$ifNull": ["$_lsn", -1]}]}
    return [{"$set": {field: {"$cond": [guard, value, f"${field}"]}
                      for field, value in set_fields.items()}}]


def build_document(values: Dict[str, Any], types: Dict[str, str],
                   pk_col: str, tracked: set, lsn: int) -> dict:
    """Build a full MongoDB document for the snapshot path. Mirrors the INSERT
    branch of change_to_op so snapshot and stream documents are identical."""
    doc = build_set_fields(values, types, tracked, lsn)
    doc["_id"] = coerce_value(types.get(pk_col, "bigint"), values[pk_col])
    doc["deleted_at"] = None
    return doc
