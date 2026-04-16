"""
license.py — License validation against the shared Supabase licenses table.

The mt4_account field in config.json maps to mt5_account in the licenses table
(same column — the field name in the DB is mt5_account but it just stores
the broker account number, which is the same concept for MT4).
"""

import asyncio
import logging
from typing import Optional

import db as supabase_db

logger = logging.getLogger(__name__)

_license_valid: bool = False


def is_license_valid() -> bool:
    return _license_valid


async def validate_license(
    pool,
    license_key: str,
    mt4_account: str,
) -> bool:
    global _license_valid
    try:
        row = await supabase_db.fetch_license_row(pool, license_key)
    except Exception as exc:
        logger.error(f"License DB query failed: {exc}")
        _license_valid = False
        return False

    if row is None:
        logger.error("License key not found in database.")
        _license_valid = False
        return False

    if row["status"] != "active":
        logger.error(f"License is {row['status']}, not active.")
        _license_valid = False
        return False

    # The DB column is named mt5_account — stores the account number
    db_account = str(row.get("mt5_account", "")).strip()
    if db_account != mt4_account.strip():
        logger.error(
            f"License is bound to account {db_account!r}, "
            f"but config says {mt4_account!r}."
        )
        _license_valid = False
        return False

    _license_valid = True
    return True


def start_heartbeat(
    pool,
    license_key: str,
    mt4_account: str,
    interval_seconds: int = 900,
) -> asyncio.Task:
    """Start background license revalidation. Returns the task (cancel on shutdown)."""

    async def _loop():
        global _license_valid
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                result = await validate_license(pool, license_key, mt4_account)
                if not result:
                    logger.warning(
                        "License heartbeat: validation failed — "
                        "new order placement will be blocked until resolved."
                    )
                else:
                    logger.debug("License heartbeat: OK.")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"License heartbeat error: {exc}")
                _license_valid = False

    return asyncio.create_task(_loop())