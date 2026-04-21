"""
tp.py — Take-profit decision engine for the MT4 Gold Auto Bot.

DESIGN
──────
The TP trail runs INSIDE the EA (MQL4) for reliability and low latency —
Python just decides WHEN to trigger it and with what parameters.

Python's role (this file):
  1. On each cycle, inspect all filled positions (grouped by signal).
  2. Find the "most profitable" position for a signal.
  3. When that position >= profit_threshold (dollars above entry), AND all
     other siblings are >= entry (breakeven), fire the TP trigger:
       - Send START_TP command to EA: partial close % + trail distance
       - EA closes partial_pct% of the profit position immediately
       - EA sets trailing stop on remainder
       - EA closes breakeven siblings at market
  4. Each ticket is only triggered once (tracked in tp_state table).

Forced closes (manual breakeven/cancel from Discord):
  Handled by ForcedExitMonitor in sync.py, which sends FORCE_CLOSE commands.

Config (config.json → "tp"):
  Same structure as MT5 bot. For gold we use:
    defaults.metals: { "type": "dollars", "value": 5.0, "trail": 2.0 }
  Scalp variant: scalp_defaults.metals
  Per-instrument overrides: overrides.XAUUSD
"""

import logging
from typing import Optional

import local_db
from comms import cmd_start_tp

logger = logging.getLogger(__name__)

GOLD_SYMBOL = "XAUUSD"


def _resolve_tp_params(cfg: dict, is_scalp: bool) -> tuple[float, float, float]:
    """
    Returns (profit_threshold_dollars, trail_dollars, partial_close_pct).

    Priority: per-instrument override > asset class default.
    For gold, asset class is always 'metals'.
    """
    tp_cfg = cfg.get("tp", {})
    partial_pct = float(tp_cfg.get("partial_close_percent", 50))

    # Per-instrument override
    overrides_key = "scalp_overrides" if is_scalp else "overrides"
    override = tp_cfg.get(overrides_key, {}).get("XAUUSD")
    if override is None:
        override = tp_cfg.get(overrides_key, {}).get("GOLD")

    if override:
        val   = float(override.get("value", 5.0))
        trail = float(override.get("trail", 2.0))
        return val, trail, partial_pct

    # Asset class default (metals)
    defaults_key = "scalp_defaults" if is_scalp else "defaults"
    metals = tp_cfg.get(defaults_key, {}).get("metals", {})
    val   = float(metals.get("value", 5.0))
    trail = float(metals.get("trail", 2.0))
    return val, trail, partial_pct


class TPEngine:
    """
    Evaluate open positions each cycle and fire TP triggers when conditions are met.
    """

    def __init__(self, config: dict, conn_path: str):
        self.config    = config
        self.conn_path = conn_path

    def run_tick(self, open_positions: list[dict]) -> None:
        """
        Called every poll cycle with the current list of open positions
        (parsed from positions.txt — only positions with status='filled'
        in local_db are relevant).

        open_positions: list of dicts from PositionSnapshot.open_positions
            keys: ticket, limit_id, signal_id, type, open_price, sl, lots, profit
        """
        if not open_positions:
            return

        # Group by signal_id
        by_signal: dict[int, list[dict]] = {}
        for pos in open_positions:
            sid = pos["signal_id"]
            by_signal.setdefault(sid, []).append(pos)

        for signal_id, positions in by_signal.items():
            self._evaluate_signal(signal_id, positions)

    def _evaluate_signal(self, signal_id: int, positions: list[dict]) -> None:
        """
        For one signal's group of open positions, decide if TP should trigger.

        Trigger conditions (mirrors MT5 bot DefaultTPStrategy):
          1. The most-recently-filled position (highest ticket number) has
             profit >= profit_threshold in dollars above entry.
          2. All other positions for the signal are >= their entry price
             (breakeven or better).

        Once triggered, sends START_TP to EA and records in tp_state.
        """
        # Filter to only positions not already under TP
        active = [p for p in positions if not local_db.is_tp_triggered(p["ticket"])]
        if not active:
            return

        # Get the mapping to know is_scalp
        # All positions for the same signal share the same scalp flag
        sample_ticket = active[0]["ticket"]
        mapping = local_db.get_mapping_by_ticket(sample_ticket)
        is_scalp = bool(mapping["is_scalp"]) if mapping else False

        threshold, trail_dollars, partial_pct = _resolve_tp_params(self.config, is_scalp)

        # Deepest position = highest ticket (placed last = furthest into the move)
        profit_pos = max(active, key=lambda p: p["ticket"])
        breakeven_siblings = [p for p in active if p["ticket"] != profit_pos["ticket"]]

        # threshold is in price-movement dollars (e.g. 2.0 = $2 price move on XAUUSD).
        # Convert account P&L to price movement: profit / (lots * 100)
        # XAUUSD: 1 lot = 100oz, so $1 price move = $100 account P&L per lot.
        profit_price_move = self._price_move(profit_pos)

        if profit_price_move < threshold:
            return  # Deepest position hasn't moved far enough yet

        # Combined sibling breakeven: sum of all siblings' P&L must be >= 0.
        # Individual siblings may be slightly negative as long as the group nets out.
        if breakeven_siblings:
            combined_sibling_pnl = sum(p.get("profit", 0.0) or 0.0 for p in breakeven_siblings)
            if combined_sibling_pnl < 0:
                logger.debug(
                    f"TP check signal {signal_id}: combined sibling P&L "
                    f"${combined_sibling_pnl:.2f} — waiting for breakeven."
                )
                return  # Siblings not at combined breakeven yet

        # All conditions met — fire TP
        logger.info(
            f"TP trigger: signal={signal_id}, profit_ticket={profit_pos['ticket']} "
            f"({profit_price_move:.2f}$ price move, threshold={threshold}$), "
            f"{len(breakeven_siblings)} sibling(s) at combined breakeven. "
            f"Sending START_TP (partial={partial_pct}%, trail={trail_dollars}$)."
        )

        # Send START_TP for the profit position
        # The EA will: partial close, trail remainder, close all siblings for this signal
        cmd_start_tp(
            self.conn_path,
            mt4_ticket=profit_pos["ticket"],
            partial_pct=partial_pct,
            trail_dollars=trail_dollars,
            signal_id=signal_id,
        )

        # Record ALL active positions for this signal as TP-triggered
        # (the EA handles siblings via the signal_id, Python just needs to
        # know not to re-trigger them)
        for pos in active:
            local_db.record_tp_triggered(
                mt4_ticket=pos["ticket"],
                signal_id=signal_id,
                limit_id=pos["limit_id"],
                partial_pct=partial_pct if pos["ticket"] == profit_pos["ticket"] else 100.0,
                trail_dollars=trail_dollars,
            )

        # Also cancel any remaining pending orders for this signal
        # (once TP fires we don't want more entries)
        pending_tickets = local_db.cancel_all_pending_for_signal(signal_id)
        for ticket in pending_tickets:
            from comms import cmd_cancel_order
            cmd_cancel_order(self.conn_path, ticket)
            logger.info(f"TP fired for signal {signal_id} — cancelled pending ticket {ticket}")

    def _price_move(self, pos: dict) -> float:
        """
        Return the price movement in dollars from entry for this position.

        The profit field from MT4 is total account P&L in account currency.
        For XAUUSD: 1 lot = 100oz, so $1 price move = $100 account P&L per lot.

        price_move = total_profit / (lots * 100)

        This matches config "value" semantics: e.g. "value": 2.0 means the
        deepest position must have moved $2 in price from its entry.
        Scale-independent — works correctly regardless of lot size.
        """
        lots = pos.get("lots", 1.0) or 1.0
        total_profit = pos.get("profit", 0.0) or 0.0
        return total_profit / (lots * 100.0)