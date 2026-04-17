"""
comms.py — File-based communication layer between the Python bot and the MT4 EA.

FILE PROTOCOL
─────────────

commands.txt  (Python → EA)
    A queue of newline-separated command lines. The EA reads + clears this file
    on each timer tick (every second). Python appends atomically. The EA processes
    ALL lines found, in order, then truncates the file.

    Command format (space-separated):
        PLACE_ORDER  <limit_id> <signal_id> <direction> <price> <sl> <lot> <expiry_type>
        CANCEL_ORDER <mt4_ticket>
        CANCEL_ALL
        CLOSE_POSITION <mt4_ticket>
        START_TP      <mt4_ticket> <partial_pct> <trail_dollars> <signal_id>
        FORCE_CLOSE   <mt4_ticket> <reason>

    direction: BUY_LIMIT | SELL_LIMIT | BUY_STOP | SELL_STOP
    expiry_type: DAY | WEEK | NONE    (maps to day_end / week_end / no_expiry)

positions.txt  (EA → Python, written every EA timer tick ~1s)
    Snapshot of all PENDING ORDERS and OPEN POSITIONS placed by this bot (filtered
    by magic number). Python reads this file every poll cycle to detect fills,
    cancellations, and SL changes made inside the terminal.

    Format — one entry per line, two sections separated by "---":

    Section 1: PENDING ORDERS
        PENDING <mt4_ticket> <limit_id> <signal_id> <type> <price> <sl> <lots>

    Section 2: OPEN POSITIONS
        POSITION <mt4_ticket> <limit_id> <signal_id> <type> <open_price> <sl> <lots> <profit>

    Separator line between sections: ---

    The EA writes an empty file (just "---\n") when there is nothing open.

    Notes:
      - limit_id and signal_id are stored in the order comment by the EA as
        "lim:<limit_id> sig:<signal_id>" — exactly as the Python side writes it.
      - type: BUY_LIMIT | SELL_LIMIT | BUY_STOP | SELL_STOP | BUY | SELL
"""

import logging
import os
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

COMMANDS_FILE  = "commands.txt"
POSITIONS_FILE = "positions.txt"


def ensure_connection_files(conn_path: str) -> None:
    """Create the connection files if they don't exist yet."""
    for filename in (COMMANDS_FILE, POSITIONS_FILE):
        fpath = os.path.join(conn_path, filename)
        if not os.path.exists(fpath):
            with open(fpath, "w", encoding="utf-8") as f:
                f.write("")
            logger.debug(f"Created {fpath}")


def _commands_path(conn_path: str) -> str:
    return os.path.join(conn_path, COMMANDS_FILE)


def _positions_path(conn_path: str) -> str:
    return os.path.join(conn_path, POSITIONS_FILE)


# ---------------------------------------------------------------------------
# Writing commands (Python → EA)
# ---------------------------------------------------------------------------

def write_command(conn_path: str, command: str, args: str) -> None:
    """
    Append one command line to commands.txt.

    Uses file-append mode so multiple commands can be queued in a single
    cycle without overwriting each other. The EA processes and clears the
    file on each timer tick.

    Format: "<COMMAND> <args>\n"
    """
    line = f"{command} {args}\n".strip() + "\n"
    path = _commands_path(conn_path)
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError as exc:
        logger.error(f"Failed to write command to {path}: {exc}")


def clear_commands_file(conn_path: str) -> None:
    """Truncate the commands file. Called on startup for a clean slate."""
    path = _commands_path(conn_path)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write("")
    except OSError as exc:
        logger.error(f"Failed to clear commands file: {exc}")


def cmd_place_order(
    conn_path: str,
    limit_id: int,
    signal_id: int,
    direction: str,
    price: float,
    sl: float,
    lot: float,
    expiry_type: str,
) -> None:
    """Queue a PLACE_ORDER command."""
    args = f"{limit_id} {signal_id} {direction} {price:.5f} {sl:.5f} {lot:.2f} {expiry_type}"
    write_command(conn_path, "PLACE_ORDER", args)
    logger.debug(f"Queued PLACE_ORDER: {args}")


def cmd_cancel_order(conn_path: str, mt4_ticket: int) -> None:
    """Queue a CANCEL_ORDER command."""
    write_command(conn_path, "CANCEL_ORDER", str(mt4_ticket))
    logger.debug(f"Queued CANCEL_ORDER: ticket={mt4_ticket}")


def cmd_cancel_all(conn_path: str) -> None:
    """Queue a CANCEL_ALL command (cancels every pending order with our magic)."""
    write_command(conn_path, "CANCEL_ALL", "")
    logger.debug("Queued CANCEL_ALL")


def cmd_close_position(conn_path: str, mt4_ticket: int) -> None:
    """Queue a CLOSE_POSITION (full market close) command."""
    write_command(conn_path, "CLOSE_POSITION", str(mt4_ticket))
    logger.debug(f"Queued CLOSE_POSITION: ticket={mt4_ticket}")


def cmd_start_tp(
    conn_path: str,
    mt4_ticket: int,
    partial_pct: float,
    trail_dollars: float,
    signal_id: int,
) -> None:
    """
    Queue a START_TP command.

    Tells the EA to:
      1. Close partial_pct% of this position immediately.
      2. Begin trailing the remainder with a trail_dollars trailing stop.
      3. Close all sibling breakeven positions for signal_id at market.

    The EA handles all of this natively in MQL4 — no further Python involvement
    needed for the trail once this command is sent.
    """
    args = f"{mt4_ticket} {partial_pct:.1f} {trail_dollars:.5f} {signal_id}"
    write_command(conn_path, "START_TP", args)
    logger.debug(f"Queued START_TP: {args}")


def cmd_force_close(conn_path: str, mt4_ticket: int, reason: str) -> None:
    """Queue a FORCE_CLOSE command (manual breakeven / cancellation)."""
    write_command(conn_path, "FORCE_CLOSE", f"{mt4_ticket} {reason}")
    logger.debug(f"Queued FORCE_CLOSE: ticket={mt4_ticket} reason={reason}")


def cmd_update_sl(conn_path: str, mt4_ticket: int, new_sl: float) -> None:
    """Queue an UPDATE_SL command to move the stop-loss on an open position."""
    write_command(conn_path, "UPDATE_SL", f"{mt4_ticket} {new_sl:.5f}")
    logger.debug(f"Queued UPDATE_SL: ticket={mt4_ticket} sl={new_sl:.5f}")


# ---------------------------------------------------------------------------
# Reading positions (EA → Python)
# ---------------------------------------------------------------------------

class PositionSnapshot:
    """Parsed snapshot from positions.txt."""

    def __init__(self):
        self.pending_orders: list[dict] = []
        self.open_positions: list[dict] = []
        self.balance: Optional[float] = None
        self.bid_price: Optional[float] = None  # ← ADD THIS

    @property
    def pending_tickets(self) -> set[int]:
        return {p["ticket"] for p in self.pending_orders}

    @property
    def position_tickets(self) -> set[int]:
        return {p["ticket"] for p in self.open_positions}


def read_positions(conn_path: str) -> Optional[PositionSnapshot]:
    """
    Read and parse positions.txt.

    Returns None if the file is missing or unreadable.
    Returns a PositionSnapshot (possibly empty) on success.

    File format:
        PENDING <ticket> <limit_id> <signal_id> <type> <price> <sl> <lots>
        ...
        ---
        POSITION <ticket> <limit_id> <signal_id> <type> <open_price> <sl> <lots> <profit>
        ...
    """
    path = _positions_path(conn_path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError as exc:
        logger.warning(f"Cannot read positions file: {exc}")
        return None

    snap = PositionSnapshot()
    in_positions_section = False

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("# BALANCE "):
            try:
                snap.balance = float(line.split()[2])
            except (IndexError, ValueError):
                pass
            continue
        if line.startswith("# BID "):
            try:
                snap.bid_price = float(line.split()[2])
            except (IndexError, ValueError):
                pass
            continue
        if line == "---":
            in_positions_section = True
            continue

        parts = line.split()
        if not parts:
            continue

        record_type = parts[0]

        if record_type == "PENDING" and len(parts) >= 8:
            try:
                snap.pending_orders.append({
                    "ticket":   int(parts[1]),
                    "limit_id": int(parts[2]),
                    "signal_id":int(parts[3]),
                    "type":     parts[4],
                    "price":    float(parts[5]),
                    "sl":       float(parts[6]),
                    "lots":     float(parts[7]),
                })
            except (ValueError, IndexError):
                logger.warning(f"Malformed PENDING line: {line!r}")

        elif record_type == "POSITION" and len(parts) >= 9:
            try:
                snap.open_positions.append({
                    "ticket":     int(parts[1]),
                    "limit_id":   int(parts[2]),
                    "signal_id":  int(parts[3]),
                    "type":       parts[4],
                    "open_price": float(parts[5]),
                    "sl":         float(parts[6]),
                    "lots":       float(parts[7]),
                    "profit":     float(parts[8]),
                })
            except (ValueError, IndexError):
                logger.warning(f"Malformed POSITION line: {line!r}")

    return snap