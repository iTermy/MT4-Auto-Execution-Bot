"""
sync.py — Core sync engine for the MT4 Gold Auto Bot.

Each run_cycle():
  1. Read positions.txt snapshot from EA.
  2. Sync EA-reported account balance.
  3. Check market hours (spread hour / weekend) — cancel/pause pending orders.
  4. Check license validity.
  5. Fetch active gold signals + pending limits from Supabase.
  6. Diff DB state vs local SQLite — place missing orders, cancel removed ones.
  7. Detect fills and external cancellations from positions snapshot.
  8. Sync SL changes on filled positions back to EA.
  9. Detect forced exits (manual breakeven/cancel after a limit fills).
 10. Run TP engine tick.

SPREAD ADJUSTMENT
─────────────────
Gold uses ICMarkets prices directly (no OANDA/Binance feed offset needed).
Both entry price and SL are adjusted for the estimated current spread:

  LONG  entry:  +spread  (BUY order triggers on ASK  → shift up)
  LONG  SL:     -spread  (MT4 closes long on BID     → push down to widen)
  SHORT entry:  -spread  (SELL order triggers on BID → shift down)
  SHORT SL:     +spread  (MT4 closes short on ASK    → push up to widen)

Python applies a conservative fixed estimate; the EA re-applies the live
bid/ask spread at execution time, overriding the estimate precisely.

LOT SIZING
──────────
  lot = (balance × risk% / 100) / (num_limits × avg_sl_distance × 100)

avg_sl_distance uses ALL limits for the signal (pending + already hit), not
just pending, so lot sizes remain equal across all entries of a multi-limit
signal even after the first limit fills.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import db as supabase_db
import local_db
from comms import (
    PositionSnapshot, read_positions,
    cmd_place_order, cmd_cancel_order,
    cmd_update_sl, cmd_force_close,
)
from tp import TPEngine

logger = logging.getLogger(__name__)

# Gold spread estimate in price units (dollars).
# Conservative: typical XAUUSD raw spread on ICMarkets is ~$0.20–0.35.
# The EA overrides with the live spread at execution time.
GOLD_SPREAD_ESTIMATE = 0.35

# Minimum SL change in DB space (dollars) to trigger an UPDATE_SL.
# Avoids constant SL writes from floating-point noise.
SL_CHANGE_TOLERANCE = 0.05


# ---------------------------------------------------------------------------
# Market hours — EST-based, no tzdata dependency (mirrors MT5 bot)
# ---------------------------------------------------------------------------

def _to_est(utc_dt: datetime) -> datetime:
    def _nth_sunday(year: int, month: int, n: int) -> int:
        first_weekday = datetime(year, month, 1).weekday()
        first_sunday  = 1 + (6 - first_weekday) % 7
        return first_sunday + (n - 1) * 7

    year = utc_dt.year
    dst_start = datetime(year, 3,  _nth_sunday(year, 3,  2), 7, 0, tzinfo=timezone.utc)
    dst_end   = datetime(year, 11, _nth_sunday(year, 11, 1), 6, 0, tzinfo=timezone.utc)
    offset = timedelta(hours=-4) if dst_start <= utc_dt < dst_end else timedelta(hours=-5)
    return utc_dt + offset


def _is_market_closed(now: datetime) -> bool:
    """Return True during spread hour (Mon–Thu 4:45–6 PM EST) and the weekend."""
    est     = _to_est(now)
    weekday = est.weekday()   # Mon=0 … Sun=6
    hm      = (est.hour, est.minute)
    if weekday == 5: return True                 # Saturday: always closed
    if weekday == 6: return hm < (18, 0)         # Sunday: closed before 6 PM
    if weekday == 4: return hm >= (16, 45)        # Friday: closed from 4:45 PM
    return (16, 45) <= hm < (18, 0)              # Mon–Thu: 4:45–6 PM spread hour


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _expiry_type_for_signal(signal: dict) -> str:
    et = (signal.get("expiry_type") or "").lower()
    if et == "day_end":  return "DAY"
    if et == "week_end": return "WEEK"
    return "NONE"


def _calculate_lot_size(
    balance: float,
    risk_percent: float,
    num_limits: int,
    avg_sl_distance: float,
    min_lot: float = 0.01,
) -> float:
    """
    Gold lot sizing: lot = (balance × risk% / 100) / (num_limits × sl_dist × 100)
    XAUUSD: 1 lot = 100 oz, $1 price move = $100/lot.
    avg_sl_distance is in price units (dollars) and uses ALL limits for equal sizing.
    """
    if avg_sl_distance <= 0 or num_limits <= 0:
        return min_lot
    raw = (balance * risk_percent / 100.0) / (num_limits * avg_sl_distance * 100.0)
    lot = max(min_lot, round(raw - (raw % 0.01), 2))  # floor to nearest 0.01
    return lot


def _spread_adjust(direction: str, price: float, sl: float, spread: float):
    """Apply spread adjustment. Returns (adj_price, adj_sl)."""
    if direction == "long":
        return price + spread, sl - spread
    else:
        return price - spread, sl + spread


# ---------------------------------------------------------------------------
# ForcedExitMonitor
# ---------------------------------------------------------------------------

class ForcedExitMonitor:
    """
    Detects when a signal is manually marked 'cancelled' or 'breakeven' in the
    DB while open positions exist. Sends FORCE_CLOSE for every affected position.
    Transition-based: only fires once per status change, not every cycle.
    """

    FORCE_EXIT_STATUSES = frozenset({"cancelled", "breakeven"})

    def __init__(self, conn_path: str):
        self.conn_path = conn_path
        self._last_status: dict[int, str] = {}

    async def check(self, pool) -> None:
        filled_signal_ids = local_db.get_signal_ids_with_filled_positions()
        if not filled_signal_ids:
            self._last_status.clear()
            return

        rows = await supabase_db.fetch_signals_by_ids(pool, list(filled_signal_ids))
        current_by_id = {r["id"]: r["status"] for r in rows}

        for signal_id in filled_signal_ids:
            current  = current_by_id.get(signal_id)
            if current is None:
                continue

            previous = self._last_status.get(signal_id)
            self._last_status[signal_id] = current

            if current not in self.FORCE_EXIT_STATUSES:
                continue
            if previous == current:
                continue  # Already acted on this transition
            if previous != "hit":
                # Signal was never 'hit' — no open positions to force-close.
                # Regular _sync_orders cancel path handles pending-only signals.
                continue

            logger.warning(
                f"ForcedExit: signal {signal_id} → {current!r} with open positions "
                f"— force-closing all."
            )
            filled = local_db.get_filled_mappings_by_signal_ids([signal_id])
            for m in filled:
                ticket = m.get("mt4_ticket")
                if ticket:
                    cmd_force_close(self.conn_path, ticket, f"manual_{current}")
                    local_db.mark_position_closed(ticket)
                    local_db.remove_tp_state(ticket)
                    logger.info(f"ForcedExit: sent FORCE_CLOSE for ticket {ticket}")

        # Prune stale entries
        self._last_status = {
            sid: st for sid, st in self._last_status.items()
            if sid in filled_signal_ids
        }


# ---------------------------------------------------------------------------
# SyncEngine
# ---------------------------------------------------------------------------

class SyncEngine:

    def __init__(self, pool, config: dict):
        self.pool      = pool
        self.config    = config
        self.conn_path = config["connection_files_path"]

        exec_cfg = config.get("execution", {})
        self.risk_percent = float(exec_cfg.get("risk_percent", 2.0))
        self.min_lot      = float(exec_cfg.get("min_lot", 0.01))

        # Account balance — seeded from config, updated each cycle from EA report
        self._cached_balance: float = float(config.get("account_balance", 10000.0))

        # Current bid price — updated each cycle from EA's # BID header
        self._cached_bid: Optional[float] = None

        # Proximity filter: only place orders for signals that have at least one
        # pending limit within this many dollars of the current bid price.
        # 0 = disabled (place all signals regardless of distance).
        self.proximity_filter_dollars: float = float(
            exec_cfg.get("proximity_filter_dollars", 50.0)
        )

        self._tp_engine           = TPEngine(config, self.conn_path)
        self._forced_exit_monitor = ForcedExitMonitor(self.conn_path)

        # Market hours state machine
        self._market_closed: Optional[bool] = None
        self._paused_limit_ids: set[int]    = set()

    # ------------------------------------------------------------------
    # Main cycle
    # ------------------------------------------------------------------

    async def run_cycle(self) -> None:
        try:
            from license import is_license_valid

            # Step 1: Always read positions snapshot first
            snap = read_positions(self.conn_path)
            if snap is not None:
                self._update_balance(snap)

            # Step 2: Market hours check (local time, no DB)
            now_closed = _is_market_closed(datetime.now(timezone.utc))
            self._handle_market_hours(now_closed)

            # Steps 3–10 only run when market is open and license is valid.
            # However, fill detection, forced exits, and TP always run so
            # open positions continue to be managed regardless.
            if snap:
                await self._detect_fills(snap)

            await self._forced_exit_monitor.check(self.pool)

            if snap:
                self._tp_engine.run_tick(snap.open_positions)

            # Placement-side work only when market is open and licensed
            if self._market_closed:
                return

            if not is_license_valid():
                logger.warning("License invalid — skipping order placement this cycle.")
                return

            # Step 4: Full sync
            await self._sync_orders()

            if snap:
                await self._sync_filled_sls(snap)

        except Exception as exc:
            logger.exception(f"Unhandled error in sync cycle: {exc}")

    # ------------------------------------------------------------------
    # Balance update
    # ------------------------------------------------------------------

    def _update_balance(self, snap: PositionSnapshot) -> None:
        if snap.balance is not None and snap.balance > 0:
            if abs(snap.balance - self._cached_balance) > 0.01:
                logger.debug(
                    f"Balance: {self._cached_balance:.2f} → {snap.balance:.2f}"
                )
                self._cached_balance = snap.balance
        if snap.bid_price is not None and snap.bid_price > 0:
            self._cached_bid = snap.bid_price

    # ------------------------------------------------------------------
    # Market hours state machine
    # ------------------------------------------------------------------

    def _handle_market_hours(self, now_closed: bool) -> None:
        prev = self._market_closed

        if prev is None:
            # First call — record state but don't act yet (avoids a spurious
            # cancel burst on startup if we happen to be inside a closed window).
            self._market_closed = now_closed
            if now_closed:
                logger.info(
                    "Startup inside a closed market window — pending orders "
                    "will be cancelled on the next cycle."
                )
            return

        if prev == now_closed:
            return  # No state change

        self._market_closed = now_closed

        if now_closed:
            logger.warning("⏰ Market closed — cancelling all pending orders.")
            self._cancel_all_for_close()
        else:
            logger.info("✅ Market reopened — restoring paused limits.")
            self._restore_paused_limits()

    def _cancel_all_for_close(self) -> None:
        pending = local_db.get_pending_mappings()
        for m in pending:
            ticket   = m.get("mt4_ticket")
            limit_id = m["limit_id"]
            if ticket:
                cmd_cancel_order(self.conn_path, ticket)
            local_db.mark_cancelled_by_limit_id(limit_id)
            self._paused_limit_ids.add(limit_id)
        if pending:
            logger.info(f"Market close: queued cancel for {len(pending)} pending order(s).")

    def _restore_paused_limits(self) -> None:
        if not self._paused_limit_ids:
            return
        deleted = local_db.delete_cancelled_for_limit_ids(list(self._paused_limit_ids))
        logger.info(
            f"Market reopen: cleared {deleted} cancelled row(s) — "
            f"orders will be re-placed next cycle."
        )
        self._paused_limit_ids.clear()

    # ------------------------------------------------------------------
    # Order sync
    # ------------------------------------------------------------------

    def _signal_is_in_proximity(self, sig: dict) -> bool:
        """
        Return True if this signal should have orders placed, based on the
        proximity filter. A signal passes if:
          - proximity_filter_dollars is 0 (filter disabled), OR
          - no current bid price is known yet (can't filter, allow through), OR
          - at least one of its pending limits is within proximity_filter_dollars
            of the current bid price.

        Signals are treated as atomic — if ANY limit qualifies, ALL limits for
        that signal are placed. This method only decides pass/fail per signal.
        """
        threshold = self.proximity_filter_dollars
        if threshold <= 0:
            return True  # Filter disabled
        if self._cached_bid is None:
            return True  # No price data yet — allow through
        bid = self._cached_bid
        for lim in sig.get("pending_limits", []):
            if abs(lim["price_level"] - bid) <= threshold:
                return True
        return False

    async def _sync_orders(self) -> None:
        """
        Main diff loop:
          • Fetch active gold signals + pending limits from DB.
          • Place orders for limits we haven't placed yet.
          • Cancel orders for limits no longer pending in DB.
          • Cancel all orders for signals that have left active/hit.
        """
        signals = await supabase_db.fetch_active_gold_signals_with_limits(self.pool)

        db_pending: dict[int, dict] = {}  # limit_id → enriched limit dict
        db_signals: dict[int, dict] = {}  # signal_id → signal dict

        for sig in signals:
            db_signals[sig["id"]] = sig
            for lim in sig.get("pending_limits", []):
                db_pending[lim["id"]] = {**lim, "_signal": sig}

        # Limit_ids already tracked (pending or filled) — never re-place
        tracked = local_db.get_all_tracked_limit_ids()

        # Pre-compute average SL distance per signal using ALL limits
        # (pending + already hit) for consistent lot sizing throughout the
        # signal's lifetime. Fetched in bulk to avoid per-signal round-trips.
        avg_sl_by_signal = await self._compute_avg_sl_distances(signals)

        # Signals that already have any tracked limit (pending or filled).
        # These bypass the proximity filter — we never want to stop managing
        # a signal we've already started placing orders for.
        already_tracked_signal_ids = local_db.get_all_tracked_signal_ids()

        # --- Place missing orders ---
        for limit_id, lim in db_pending.items():
            if limit_id in tracked:
                continue
            sig = lim["_signal"]
            signal_id = sig["id"]

            # Proximity filter: skip new signals whose closest limit is too
            # far from the current price. Signals already being tracked are
            # always allowed through so existing order management is unaffected.
            if signal_id not in already_tracked_signal_ids:
                if not self._signal_is_in_proximity(sig):
                    logger.debug(
                        f"Signal {signal_id}: all limits outside "
                        f"{self.proximity_filter_dollars:.0f}$ proximity "
                        f"(bid={self._cached_bid}) — skipping placement."
                    )
                    continue

            self._place_order(lim, sig, avg_sl_by_signal.get(signal_id))

        # --- Cancel orders for limits no longer pending in DB ---
        local_pending = local_db.get_pending_mappings()
        for m in local_pending:
            limit_id = m["limit_id"]
            if limit_id not in db_pending:
                ticket = m.get("mt4_ticket")
                logger.info(
                    f"Limit {limit_id} no longer pending in DB — cancelling"
                    + (f" ticket {ticket}" if ticket else " (no ticket yet)")
                )
                if ticket:
                    cmd_cancel_order(self.conn_path, ticket)
                local_db.mark_cancelled_by_limit_id(limit_id)

        # --- Cancel all orders for signals that left active/hit ---
        active_ids = {s["id"] for s in signals}
        for signal_id in local_db.get_all_tracked_signal_ids():
            if signal_id not in active_ids:
                tickets = local_db.cancel_all_pending_for_signal(signal_id)
                for ticket in tickets:
                    logger.info(
                        f"Signal {signal_id} left active — cancelling ticket {ticket}"
                    )
                    cmd_cancel_order(self.conn_path, ticket)

    async def _compute_avg_sl_distances(
        self, signals: list[dict]
    ) -> dict[int, float]:
        """
        For each signal, compute the average SL distance across ALL its limits
        (not just pending ones). This keeps lot sizes consistent even after the
        first limit of a multi-limit signal fills.

        Queries Supabase for all limits per signal. For signals with only one
        pending limit or where all limits are pending, the result is identical
        to using only pending limits.
        """
        result: dict[int, float] = {}
        for sig in signals:
            signal_id = sig["id"]
            db_sl     = sig["stop_loss"]
            try:
                all_lims = await supabase_db.fetch_all_limits_for_signal(
                    self.pool, signal_id
                )
            except Exception as exc:
                logger.warning(
                    f"Could not fetch all limits for signal {signal_id}: {exc} "
                    f"— falling back to pending limits only."
                )
                all_lims = sig.get("pending_limits", [])

            if not all_lims:
                continue

            distances = [abs(l["price_level"] - db_sl) for l in all_lims]
            result[signal_id] = sum(distances) / len(distances)

        return result

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    def _place_order(
        self,
        lim: dict,
        sig: dict,
        avg_sl_distance: Optional[float],
    ) -> None:
        """Queue a PLACE_ORDER command and record it in local_db."""
        direction  = sig["direction"]
        db_price   = lim["price_level"]
        db_sl      = sig["stop_loss"]
        num_limits = sig.get("total_limits", 1) or 1
        is_scalp   = bool(sig.get("scalp", False))
        expiry     = _expiry_type_for_signal(sig)

        # Spread adjustment
        adj_price, adj_sl = _spread_adjust(direction, db_price, db_sl, GOLD_SPREAD_ESTIMATE)

        # Lot size using average SL distance (DB space) across all signal limits
        sl_dist = avg_sl_distance if avg_sl_distance is not None else abs(db_price - db_sl)
        if sl_dist <= 0:
            logger.warning(
                f"Signal {sig['id']} limit {lim['id']}: SL distance is zero — "
                f"using min_lot."
            )
            sl_dist = 0.01  # forces min_lot

        lot = _calculate_lot_size(
            balance         = self._cached_balance,
            risk_percent    = self.risk_percent,
            num_limits      = num_limits,
            avg_sl_distance = sl_dist,
            min_lot         = self.min_lot,
        )

        # Tell the EA to attempt BUY_LIMIT or SELL_LIMIT. The EA will upgrade
        # to BUY_STOP / SELL_STOP automatically if price has already passed
        # the limit level by the time the command is processed.
        order_type = "BUY_LIMIT" if direction == "long" else "SELL_LIMIT"

        cmd_place_order(
            conn_path   = self.conn_path,
            limit_id    = lim["id"],
            signal_id   = sig["id"],
            direction   = order_type,
            price       = adj_price,
            sl          = adj_sl,
            lot         = lot,
            expiry_type = expiry,
        )

        local_db.insert_order_mapping(
            limit_id     = lim["id"],
            signal_id    = sig["id"],
            order_type   = order_type.lower(),
            lot_size     = lot,
            price        = adj_price,
            db_stop_loss = db_sl,
            is_scalp     = is_scalp,
        )

        logger.info(
            f"Queued {order_type}: "
            f"signal={sig['id']} limit={lim['id']} "
            f"price={adj_price:.2f} sl={adj_sl:.2f} "
            f"lot={lot:.2f} expiry={expiry} scalp={is_scalp}"
        )

    # ------------------------------------------------------------------
    # Fill detection
    # ------------------------------------------------------------------

    async def _detect_fills(self, snap: PositionSnapshot) -> None:
        """
        Reconcile local pending mappings against the EA's positions snapshot.

        For each locally-pending row:
          • No ticket yet + appears in snap.pending_orders  → assign ticket
          • Has ticket + ticket in snap.open_positions       → mark filled
          • Has ticket + ticket in snap.pending_orders       → still pending, OK
          • Has ticket + ticket absent from both sections   → external cancel/expiry
        """
        local_pending = local_db.get_pending_mappings()
        if not local_pending:
            return

        pending_tickets  = snap.pending_tickets
        position_tickets = snap.position_tickets

        # Index EA pending orders by limit_id for ticket assignment
        ea_pending_by_limit: dict[int, dict] = {
            p["limit_id"]: p for p in snap.pending_orders
        }

        for m in local_pending:
            limit_id = m["limit_id"]
            ticket   = m.get("mt4_ticket")

            # --- Assign ticket if not yet known ---
            if ticket is None:
                ea_order = ea_pending_by_limit.get(limit_id)
                if ea_order:
                    ticket = ea_order["ticket"]
                    local_db.set_ticket(limit_id, ticket)
                    logger.debug(
                        f"Ticket assigned: limit_id={limit_id} → ticket={ticket}"
                    )
                else:
                    # Command not yet processed by EA (within-second latency)
                    continue

            # --- Check if filled ---
            if ticket in position_tickets:
                logger.info(
                    f"Fill: ticket={ticket} "
                    f"limit_id={limit_id} signal_id={m['signal_id']}"
                )
                local_db.mark_filled(ticket)
                continue

            # --- Check if still pending ---
            if ticket in pending_tickets:
                continue

            # --- Ticket absent from both sections → external cancel / expiry ---
            logger.info(
                f"Order vanished (cancelled/expired externally): "
                f"ticket={ticket} limit_id={limit_id}"
            )
            local_db.mark_cancelled_by_ticket(ticket)

    # ------------------------------------------------------------------
    # SL sync for filled positions
    # ------------------------------------------------------------------

    async def _sync_filled_sls(self, snap: PositionSnapshot) -> None:
        """
        When the signal's stop_loss is updated in the DB (e.g. the signal sender
        edits the original Discord message), push the new SL to the EA for all
        filled positions that are NOT yet under TP trail management.

        Positions under TP trail are skipped — the EA's trailing stop owns the
        SL from that point and overwriting it would break the trail.
        """
        active_signals = await supabase_db.fetch_active_gold_signals(self.pool)
        if not active_signals:
            return

        active_by_id = {s["id"]: s for s in active_signals}
        filled = local_db.get_filled_mappings_by_signal_ids(list(active_by_id.keys()))

        if not filled:
            return

        for m in filled:
            ticket    = m.get("mt4_ticket")
            signal_id = m["signal_id"]
            if not ticket:
                continue

            # TP engine owns the SL once START_TP has been sent
            if local_db.is_tp_triggered(ticket):
                continue

            signal = active_by_id.get(signal_id)
            if not signal:
                continue

            db_sl     = signal["stop_loss"]
            stored_sl = m.get("db_stop_loss")

            if stored_sl is None:
                # First cycle after fill — record baseline, act next cycle
                local_db.update_db_stop_loss(ticket, db_sl)
                continue

            if abs(db_sl - stored_sl) < SL_CHANGE_TOLERANCE:
                continue  # No meaningful change

            # SL has changed — apply spread adjustment and push to EA
            direction = signal["direction"]
            _, mt4_sl = _spread_adjust(direction, db_sl, db_sl, GOLD_SPREAD_ESTIMATE)
            # _spread_adjust on SL only: for long, mt4_sl = db_sl - spread; for short, db_sl + spread
            if direction == "long":
                mt4_sl = db_sl - GOLD_SPREAD_ESTIMATE
            else:
                mt4_sl = db_sl + GOLD_SPREAD_ESTIMATE

            logger.info(
                f"SL update: signal={signal_id} ticket={ticket} "
                f"{stored_sl:.2f} → {db_sl:.2f} (mt4_sl={mt4_sl:.2f})"
            )
            cmd_update_sl(self.conn_path, ticket, mt4_sl)
            local_db.update_db_stop_loss(ticket, db_sl)
            local_db.update_current_sl(ticket, mt4_sl)