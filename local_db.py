"""
local_db.py — SQLite helpers for orders.db.

Tracks:
  - order_mappings: pending orders and filled positions we placed via the EA.
  - tp_state: which positions are currently being managed by the EA's TP trail.

Never touches Supabase. Per-user, local only.
"""

import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = "orders.db"

DDL = """
CREATE TABLE IF NOT EXISTS order_mappings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    limit_id        BIGINT  NOT NULL UNIQUE,
    signal_id       BIGINT  NOT NULL,
    mt4_ticket      BIGINT  UNIQUE,          -- NULL until EA confirms via positions.txt
    order_type      TEXT    NOT NULL,        -- 'buy_limit'|'sell_limit'|'buy_stop'|'sell_stop'
    lot_size        REAL,
    price           REAL,                    -- spread-adjusted entry price we sent
    db_stop_loss    REAL,                    -- signal stop_loss (DB space) at placement
    current_sl      REAL,                    -- MT4-space SL last applied
    placed_at       TEXT    NOT NULL,
    filled_at       TEXT,
    cancelled_at    TEXT,
    status          TEXT    NOT NULL DEFAULT 'pending',
    is_scalp        INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_om_signal_id ON order_mappings(signal_id);
CREATE INDEX IF NOT EXISTS idx_om_status    ON order_mappings(status);
CREATE INDEX IF NOT EXISTS idx_om_ticket    ON order_mappings(mt4_ticket);

-- Tracks positions handed off to the EA TP engine.
-- Once START_TP is sent, the EA owns the trail. This table prevents re-triggering
-- and lets ForcedExitMonitor know which tickets to FORCE_CLOSE.
CREATE TABLE IF NOT EXISTS tp_state (
    mt4_ticket      BIGINT  PRIMARY KEY,
    signal_id       BIGINT  NOT NULL,
    limit_id        BIGINT  NOT NULL,
    tp_triggered_at TEXT    NOT NULL,
    partial_pct     REAL    NOT NULL,
    trail_dollars   REAL    NOT NULL
);
"""

DDL_MIGRATIONS = [
    "ALTER TABLE order_mappings ADD COLUMN price REAL;",
    "ALTER TABLE order_mappings ADD COLUMN current_sl REAL;",
]


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.executescript(DDL)
        for stmt in DDL_MIGRATIONS:
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError:
                pass  # column already exists
        conn.commit()
    logger.debug(f"Local DB initialised at '{db_path}'.")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row(r) -> Optional[dict]:
    return dict(r) if r else None


def _rows(rs) -> list[dict]:
    return [dict(r) for r in rs]


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

def insert_order_mapping(
    limit_id: int,
    signal_id: int,
    order_type: str,
    lot_size: float,
    price: float,
    db_stop_loss: float,
    is_scalp: bool = False,
    db_path: str = DB_PATH,
) -> None:
    """
    Record a pending order queued to the EA. mt4_ticket is NULL until the
    EA confirms the order exists in positions.txt.
    Uses INSERT OR IGNORE — safe to call multiple times for the same limit_id
    (idempotent, prevents double-placement within a single cycle).
    """
    sql = """
        INSERT OR IGNORE INTO order_mappings
            (limit_id, signal_id, order_type, lot_size, price, db_stop_loss,
             current_sl, placed_at, status, is_scalp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
    """
    with get_conn(db_path) as conn:
        conn.execute(sql, (
            limit_id, signal_id, order_type, lot_size, price,
            db_stop_loss, db_stop_loss, _now(), int(is_scalp),
        ))
        conn.commit()


def set_ticket(limit_id: int, mt4_ticket: int, db_path: str = DB_PATH) -> None:
    """Assign a ticket once the EA confirms the order exists in positions.txt."""
    sql = "UPDATE order_mappings SET mt4_ticket = ? WHERE limit_id = ? AND mt4_ticket IS NULL"
    with get_conn(db_path) as conn:
        conn.execute(sql, (mt4_ticket, limit_id))
        conn.commit()


# ---------------------------------------------------------------------------
# Status transitions
# ---------------------------------------------------------------------------

def mark_filled(mt4_ticket: int, db_path: str = DB_PATH) -> None:
    sql = """
        UPDATE order_mappings
        SET status = 'filled', filled_at = ?
        WHERE mt4_ticket = ? AND status = 'pending'
    """
    with get_conn(db_path) as conn:
        conn.execute(sql, (_now(), mt4_ticket))
        conn.commit()
    logger.debug(f"Marked ticket {mt4_ticket} as filled.")


def mark_cancelled_by_ticket(mt4_ticket: int, db_path: str = DB_PATH) -> None:
    """Mark a PENDING order as cancelled (e.g. external cancel or expiry)."""
    sql = """
        UPDATE order_mappings
        SET status = 'cancelled', cancelled_at = ?
        WHERE mt4_ticket = ? AND status = 'pending'
    """
    with get_conn(db_path) as conn:
        conn.execute(sql, (_now(), mt4_ticket))
        conn.commit()
    logger.debug(f"Marked ticket {mt4_ticket} as cancelled.")


def mark_position_closed(mt4_ticket: int, db_path: str = DB_PATH) -> None:
    """
    Mark a FILLED position as closed after a FORCE_CLOSE command was sent.
    Uses 'cancelled' status to distinguish from open positions in queries.
    """
    sql = """
        UPDATE order_mappings
        SET status = 'cancelled', cancelled_at = ?
        WHERE mt4_ticket = ? AND status = 'filled'
    """
    with get_conn(db_path) as conn:
        conn.execute(sql, (_now(), mt4_ticket))
        conn.commit()
    logger.debug(f"Marked filled position ticket {mt4_ticket} as closed.")


def mark_cancelled_by_limit_id(limit_id: int, db_path: str = DB_PATH) -> None:
    """Cancel a pending mapping by limit_id (before ticket is assigned, or by limit)."""
    sql = """
        UPDATE order_mappings
        SET status = 'cancelled', cancelled_at = ?
        WHERE limit_id = ? AND status = 'pending'
    """
    with get_conn(db_path) as conn:
        conn.execute(sql, (_now(), limit_id))
        conn.commit()


def cancel_all_pending_for_signal(signal_id: int, db_path: str = DB_PATH) -> list[int]:
    """
    Mark all pending rows for a signal as cancelled.
    Returns list of mt4_tickets (non-None only) for sending CANCEL_ORDER commands.
    """
    sql_sel = """
        SELECT limit_id, mt4_ticket FROM order_mappings
        WHERE signal_id = ? AND status = 'pending'
    """
    sql_upd = """
        UPDATE order_mappings
        SET status = 'cancelled', cancelled_at = ?
        WHERE signal_id = ? AND status = 'pending'
    """
    with get_conn(db_path) as conn:
        rows = _rows(conn.execute(sql_sel, (signal_id,)).fetchall())
        conn.execute(sql_upd, (_now(), signal_id))
        conn.commit()

    return [r["mt4_ticket"] for r in rows if r["mt4_ticket"] is not None]


def update_current_sl(mt4_ticket: int, sl: float, db_path: str = DB_PATH) -> None:
    """Track the MT4-space SL last applied on a filled position."""
    sql = "UPDATE order_mappings SET current_sl = ? WHERE mt4_ticket = ?"
    with get_conn(db_path) as conn:
        conn.execute(sql, (sl, mt4_ticket))
        conn.commit()


def update_db_stop_loss(mt4_ticket: int, db_sl: float, db_path: str = DB_PATH) -> None:
    """Update stored db_stop_loss baseline after syncing a new SL to the EA."""
    sql = "UPDATE order_mappings SET db_stop_loss = ? WHERE mt4_ticket = ?"
    with get_conn(db_path) as conn:
        conn.execute(sql, (db_sl, mt4_ticket))
        conn.commit()


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def get_pending_mappings(db_path: str = DB_PATH) -> list[dict]:
    sql = "SELECT * FROM order_mappings WHERE status = 'pending'"
    with get_conn(db_path) as conn:
        return _rows(conn.execute(sql).fetchall())


def get_mapping_by_limit_id(limit_id: int, db_path: str = DB_PATH) -> Optional[dict]:
    sql = "SELECT * FROM order_mappings WHERE limit_id = ?"
    with get_conn(db_path) as conn:
        return _row(conn.execute(sql, (limit_id,)).fetchone())


def get_mapping_by_ticket(ticket: int, db_path: str = DB_PATH) -> Optional[dict]:
    sql = "SELECT * FROM order_mappings WHERE mt4_ticket = ?"
    with get_conn(db_path) as conn:
        return _row(conn.execute(sql, (ticket,)).fetchone())


def get_filled_mappings_by_signal_ids(
    signal_ids: list[int], db_path: str = DB_PATH
) -> list[dict]:
    if not signal_ids:
        return []
    ph = ",".join("?" * len(signal_ids))
    sql = f"SELECT * FROM order_mappings WHERE signal_id IN ({ph}) AND status = 'filled'"
    with get_conn(db_path) as conn:
        return _rows(conn.execute(sql, signal_ids).fetchall())


def get_all_tracked_limit_ids(db_path: str = DB_PATH) -> set[int]:
    """
    Limit_ids with pending or filled mappings.
    Excludes cancelled/error so re-placement works after a cancel+restore cycle.
    """
    sql = "SELECT DISTINCT limit_id FROM order_mappings WHERE status IN ('pending', 'filled')"
    with get_conn(db_path) as conn:
        return {r["limit_id"] for r in conn.execute(sql).fetchall()}


def get_all_tracked_signal_ids(db_path: str = DB_PATH) -> set[int]:
    sql = "SELECT DISTINCT signal_id FROM order_mappings"
    with get_conn(db_path) as conn:
        return {r["signal_id"] for r in conn.execute(sql).fetchall()}


def get_signal_ids_with_filled_positions(db_path: str = DB_PATH) -> set[int]:
    sql = "SELECT DISTINCT signal_id FROM order_mappings WHERE status = 'filled'"
    with get_conn(db_path) as conn:
        return {r["signal_id"] for r in conn.execute(sql).fetchall()}


def purge_all_pending_on_startup(db_path: str = DB_PATH) -> tuple[int, int]:
    """
    Wipe pending/cancelled rows on startup for a clean slate.
    Filled rows (open positions) are intentionally preserved — they are
    re-detected by the TP engine on the first cycle via positions.txt.
    Returns (orders_deleted, tp_state_rows_deleted).
    """
    with get_conn(db_path) as conn:
        c1 = conn.execute(
            "DELETE FROM order_mappings WHERE status IN ('pending', 'cancelled', 'error')"
        )
        c2 = conn.execute("DELETE FROM tp_state")
        conn.commit()
    return c1.rowcount, c2.rowcount


def delete_cancelled_for_limit_ids(
    limit_ids: list[int], db_path: str = DB_PATH
) -> int:
    """
    Delete 'cancelled' rows for the given limit_ids so the sync engine sees
    them as untracked and re-places them (used after market-close restore).
    """
    if not limit_ids:
        return 0
    ph = ",".join("?" * len(limit_ids))
    sql = f"""
        DELETE FROM order_mappings
        WHERE limit_id IN ({ph}) AND status = 'cancelled'
    """
    with get_conn(db_path) as conn:
        cur = conn.execute(sql, limit_ids)
        conn.commit()
    return cur.rowcount


# ---------------------------------------------------------------------------
# TP state
# ---------------------------------------------------------------------------

def record_tp_triggered(
    mt4_ticket: int,
    signal_id: int,
    limit_id: int,
    partial_pct: float,
    trail_dollars: float,
    db_path: str = DB_PATH,
) -> None:
    """Record that we've handed this ticket to the EA TP engine."""
    sql = """
        INSERT OR REPLACE INTO tp_state
            (mt4_ticket, signal_id, limit_id, tp_triggered_at, partial_pct, trail_dollars)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    with get_conn(db_path) as conn:
        conn.execute(sql, (mt4_ticket, signal_id, limit_id, _now(), partial_pct, trail_dollars))
        conn.commit()


def is_tp_triggered(mt4_ticket: int, db_path: str = DB_PATH) -> bool:
    sql = "SELECT 1 FROM tp_state WHERE mt4_ticket = ?"
    with get_conn(db_path) as conn:
        return conn.execute(sql, (mt4_ticket,)).fetchone() is not None


def get_tp_tickets_for_signal(signal_id: int, db_path: str = DB_PATH) -> list[int]:
    sql = "SELECT mt4_ticket FROM tp_state WHERE signal_id = ?"
    with get_conn(db_path) as conn:
        return [r["mt4_ticket"] for r in conn.execute(sql, (signal_id,)).fetchall()]


def remove_tp_state(mt4_ticket: int, db_path: str = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.execute("DELETE FROM tp_state WHERE mt4_ticket = ?", (mt4_ticket,))
        conn.commit()