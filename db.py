"""
db.py — Supabase / asyncpg read-only helpers.

Gold-only: only queries signals and limits for XAUUSD / GOLD instruments.
Read-only role (execution_bot_ro) — no INSERT/UPDATE/DELETE.
"""

import asyncpg
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Hardcoded read-only DSN — bot is not compiled, but this role is SELECT-only
# on signals, limits, licenses. No sensitive write access possible.
_RO_DSN = "postgresql://execution_bot_ro.cqogevbfbrfzgbuxbhmn:oS%2495chu86HanS@aws-1-us-east-2.pooler.supabase.com:5432/postgres"  # Set your Supabase session pooler URL here


async def create_pool() -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        dsn=_RO_DSN,
        min_size=1,
        max_size=3,
        server_settings={"search_path": "public"},
    )
    logger.debug("Supabase pool created.")
    return pool


async def close_pool(pool: asyncpg.Pool) -> None:
    await pool.close()


def _row(record) -> dict:
    return dict(record) if record else None


def _rows(records) -> list[dict]:
    return [dict(r) for r in records]


# ---------------------------------------------------------------------------
# Gold signal + limit queries
# ---------------------------------------------------------------------------

_GOLD_INSTRUMENTS = ("XAUUSD", "GOLD")

async def fetch_active_gold_signals_with_limits(pool: asyncpg.Pool) -> list[dict]:
    """
    Return all active/hit XAUUSD/GOLD signals, each with a 'pending_limits' list.
    This is the primary query — one round-trip per cycle.
    """
    query = """
        SELECT s.id, s.instrument, s.direction, s.stop_loss,
               s.expiry_type, s.expiry_time, s.status,
               s.total_limits, s.limits_hit, s.scalp,
               s.created_at, s.updated_at,
               l.id          AS lim_id,
               l.price_level AS lim_price,
               l.sequence_number AS lim_seq,
               l.status      AS lim_status
        FROM signals s
        JOIN limits l ON l.signal_id = s.id
        WHERE s.status IN ('active', 'hit')
          AND UPPER(s.instrument) IN ('XAUUSD', 'GOLD')
          AND l.status = 'pending'
        ORDER BY s.id, l.sequence_number
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    signals: dict[int, dict] = {}
    for r in rows:
        sid = r["id"]
        if sid not in signals:
            signals[sid] = {
                "id":           r["id"],
                "instrument":   r["instrument"],
                "direction":    r["direction"],
                "stop_loss":    r["stop_loss"],
                "expiry_type":  r["expiry_type"],
                "expiry_time":  r["expiry_time"],
                "status":       r["status"],
                "total_limits": r["total_limits"],
                "limits_hit":   r["limits_hit"],
                "scalp":        r["scalp"],
                "created_at":   r["created_at"],
                "updated_at":   r["updated_at"],
                "pending_limits": [],
            }
        signals[sid]["pending_limits"].append({
            "id":              r["lim_id"],
            "signal_id":       sid,
            "price_level":     r["lim_price"],
            "sequence_number": r["lim_seq"],
            "status":          r["lim_status"],
        })

    return list(signals.values())


async def fetch_active_gold_signals(pool: asyncpg.Pool) -> list[dict]:
    """Return active/hit gold signals without limit detail. Used for SL sync."""
    query = """
        SELECT id, instrument, direction, stop_loss,
               expiry_type, expiry_time, status,
               total_limits, limits_hit, scalp,
               created_at, updated_at
        FROM signals
        WHERE status IN ('active', 'hit')
          AND UPPER(instrument) IN ('XAUUSD', 'GOLD')
        ORDER BY id
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)
    return _rows(rows)


async def fetch_signals_by_ids(pool: asyncpg.Pool, signal_ids: list[int]) -> list[dict]:
    """Fetch signals by PK list (any status — used for forced-exit detection)."""
    if not signal_ids:
        return []
    query = """
        SELECT id, instrument, direction, stop_loss, status, scalp,
               expiry_type, expiry_time, total_limits, limits_hit
        FROM signals
        WHERE id = ANY($1::bigint[])
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, signal_ids)
    return _rows(rows)


async def fetch_all_limits_for_signal(pool: asyncpg.Pool, signal_id: int) -> list[dict]:
    """All limits for a signal (any status). Used for average SL distance calc."""
    query = """
        SELECT id, signal_id, price_level, sequence_number, status
        FROM limits
        WHERE signal_id = $1
        ORDER BY sequence_number
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, signal_id)
    return _rows(rows)


# ---------------------------------------------------------------------------
# License
# ---------------------------------------------------------------------------

async def fetch_license_row(
    pool: asyncpg.Pool, license_key: str
) -> Optional[dict]:
    query = """
        SELECT license_key, mt5_account, status
        FROM licenses
        WHERE license_key = $1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, license_key)
    return _row(row)