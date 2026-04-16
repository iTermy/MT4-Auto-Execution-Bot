"""
main.py — MT4 Gold Auto Bot entry point.

No GUI, no compilation. Run with: python main.py
Configure via config.json.
"""

import asyncio
import json
import logging
import os
import sys

import db as supabase_db
import local_db
import license as license_mod
from sync import SyncEngine
from comms import write_command, clear_commands_file, ensure_connection_files

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

POLL_INTERVAL = 5  # seconds, hardcoded


def load_config(path: str = "config.json") -> dict:
    if not os.path.exists(path):
        logger.error(f"config.json not found at {path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_config(cfg: dict) -> None:
    errors = []
    if not cfg.get("license", {}).get("key", "").strip():
        errors.append("license.key is empty")
    if not str(cfg.get("mt4_account", "")).strip():
        errors.append("mt4_account is missing")
    if not cfg.get("connection_files_path", "").strip():
        errors.append("connection_files_path is missing")
    elif not os.path.isdir(cfg["connection_files_path"]):
        errors.append(f"connection_files_path does not exist: {cfg['connection_files_path']}")
    if errors:
        for e in errors:
            logger.error(f"Config error: {e}")
        sys.exit(1)


async def main() -> None:
    cfg = load_config()
    validate_config(cfg)

    license_key = cfg["license"]["key"].strip()
    mt4_account = str(cfg["mt4_account"]).strip()
    conn_path   = cfg["connection_files_path"].strip()

    ensure_connection_files(conn_path)

    logger.info("=" * 60)
    logger.info("MT4 Gold Auto Bot starting")
    logger.info(f"MT4 account : {mt4_account}")
    logger.info(f"Comms path  : {conn_path}")

    # Supabase
    logger.info("Connecting to Supabase...")
    try:
        pool = await supabase_db.create_pool()
    except Exception as exc:
        logger.error(f"Supabase connection failed: {exc}")
        sys.exit(1)
    logger.info("Supabase connected.")

    # License
    logger.info("Validating license...")
    if not await license_mod.validate_license(pool, license_key, mt4_account):
        logger.error("License invalid — exiting.")
        await supabase_db.close_pool(pool)
        sys.exit(1)
    logger.info("License valid.")

    # Local DB
    local_db.init_db()

    # Startup: wipe stale state + tell EA to cancel everything
    orders_purged, tp_purged = local_db.purge_all_pending_on_startup()
    if orders_purged:
        logger.info(f"Purged {orders_purged} stale pending row(s) from local DB.")

    clear_commands_file(conn_path)
    write_command(conn_path, "CANCEL_ALL", "")
    logger.info("Sent CANCEL_ALL to EA for clean startup.")

    # License heartbeat
    heartbeat = license_mod.start_heartbeat(pool, license_key, mt4_account)

    # Poll loop
    engine = SyncEngine(pool, cfg)
    logger.info(f"Poll loop started (interval={POLL_INTERVAL}s). Press Ctrl+C to stop.")

    try:
        while True:
            await engine.run_cycle()
            await asyncio.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as exc:
        logger.exception(f"Fatal error: {exc}")
    finally:
        heartbeat.cancel()
        try:
            await heartbeat
        except asyncio.CancelledError:
            pass
        await supabase_db.close_pool(pool)
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())