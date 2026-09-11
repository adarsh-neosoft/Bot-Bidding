"""Per-auction item worker pool.

Each WS snapshot triggers process_snapshot, which for every (auction, item)
pair cancels any previous worker and spawns a fresh thread running
process_item. A worker goes through:

    assign bot -> fetch snapshot fields -> decide (persona, delay)
       -> wait (interruptible) -> re-check live best -> submit bid

Workers do NOT self-reschedule. The server's echo of our own place_bid (or
another bidder's bid) arrives as a fresh snapshot and drives the next
cycle.

Bot identity
------------
When running under bot_runner, the subprocess is pinned to one bot via the
BOT_USERNAME env var; `_ensure_bot_assigned` uses that identity instead of
picking randomly from the legacy pool.

Cross-bot coordination
----------------------
Multiple bots run in separate processes for the same auction. Before each
submit:
  * try_acquire_bid_mutex: short-lived Redis lock per (auction, item).
    Only one of our bots submits at a time, preventing same-millisecond
    duplicates at the same price.
  * get_recent_our_bid: if our other bot already submitted within
    OUR_BID_SKIP_WINDOW_SEC, this worker skips its submit and waits for
    the next snapshot.
"""

import logging
import os
import random
import threading
import time
from datetime import datetime, timedelta
from threading import Event
from typing import Any, Dict

from AucApi import (
    # get_current_bidding_details,
    submit_bid,
)
import AucApi

from app.services.decision_service import DecisionService
from config import (
    OUR_BID_SKIP_WINDOW_SEC,
    bot_user_ids,
)
from state_store import (
    assign_bot,
    assigned_bot_for,
    get_auction_times,
    get_bot_state,
    get_recent_our_bid,
    is_bot_assigned,
    is_bots_disabled,
    mark_our_bid,
    persist_bot_state,
    release_bid_mutex,
    try_acquire_bid_mutex,
    worker_cancel_events,
    worker_timers,
)

from logger_directory import AuctionPlayer
logger = AuctionPlayer

DECISION = DecisionService()

# When launched by bot_runner, the subprocess is pinned to one bot via env.
# Workers honour this instead of random-picking from bot_user_ids.
BOT_ID_OVERRIDE = os.getenv("BOT_USERNAME") or None


# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------
def process_external_snapshot(snapshot: Dict[str, Any]) -> None:
    """Adapter entry point used by ws_adapter."""
    try:
        process_snapshot(snapshot)
    except Exception:
        logger.exception("Error in process_external_snapshot")


def _fetch_item_block(auc: Dict[str, Any], item_id: Any) -> Dict[str, Any]:
    """Return the item sub-dict from an auction snapshot.

    Item keys can be int or str depending on the upstream payload; fall back
    to the first item block if nothing matches.
    """
    items = auc.get("AUCTION_ITEMS") or {}
    try:
        if item_id in items:
            return items[item_id]
        if str(item_id) in items:
            return items[str(item_id)]
        for k, v in items.items():
            try:
                if int(k) == int(item_id):
                    return v
            except Exception:
                pass
    except Exception:
        pass

    if isinstance(items, dict) and items:
        return next(iter(items.values()))
    return {}


def _safe_get_current_best(fallback: float) -> float:
    """Fetch the live current best price; return `fallback` on any failure."""
    # try:
    #     if hasattr(AucApi, "get_current_bidding_details"):
    #         details = get_current_bidding_details(auc_id, item_id)
    #         if isinstance(details, dict):
    #             for key in ("best_price", "current_bid", "current_highest", "current_price"):
    #                 if key in details:
    #                     return float(details[key] or 0)
    #         if isinstance(details, (int, float)):
    #             return float(details)
    # except Exception:
    #     logger.debug(
    #         "Could not fetch live current bid for %s:%s (falling back).",
    #         auc_id, item_id, exc_info=True,
    #     )
    return float(fallback or 0.0)


# ---------------------------------------------------------------------------
# Worker field extraction
# ---------------------------------------------------------------------------
def _extract_snapshot_fields(auc_id: str, item_block: Dict[str, Any], auc: Dict[str, Any]) -> Dict[str, float]:
    """Pull the numeric fields the worker needs from a snapshot.

    Duration fields:
        The WS payload rarely carries remaining/total_duration. If absent,
        we fall back to auction-level times persisted by the scheduler
        (auction:<aid> hash fields start_ts/expiry_ts). As a last resort
        default to 1s so divisions don't blow up; that used to silently
        bias persona selection toward 'patient' on every snapshot.
    """
    total_duration = float(item_block.get("total_duration", auc.get("total_duration", 0)))
    remaining_duration = float(item_block.get("remaining_duration", auc.get("remaining_duration", 0)))

    if total_duration <= 0 or remaining_duration <= 0:
        times = get_auction_times(auc_id)
        if times:
            start_ts = times["start_ts"]
            expiry_ts = times["expiry_ts"]
            if expiry_ts > start_ts:
                total_duration = expiry_ts - start_ts
                remaining_duration = max(0.0, expiry_ts - time.time())

    if total_duration <= 0:
        total_duration = 1.0
    if remaining_duration <= 0:
        remaining_duration = 1.0

    return {
        "snapshot_best": float(item_block.get("BEST_PRICE", auc.get("BEST_PRICE", 0))),
        "snapshot_threshold": float(
            item_block.get(
                "THRESHOLD_PRICE1",
                item_block.get(
                    "THRESHOLD_PRICE",
                    auc.get("THRESHOLD_PRICE1", auc.get("THRESHOLD_PRICE", 0) or 0),
                ),
            )
        ),
        "min_allowed": float(item_block.get("MIN_BID_AMOUNT", auc.get("MIN_BID_AMOUNT", 1))),
        "total_duration": total_duration,
        "remaining_duration": remaining_duration,
    }


def _build_decide_request(auc_id, item_key, bot_id, auc, item_block, fields, best_now):
    """Construct the duck-typed request object expected by DecisionService."""
    class _Req:
        pass

    # Compute phase_ratio / time_left_pct ourselves from duration fields.
    # The server rarely populates these on item_block, and falling back to 0.0
    # biases select_persona toward 'patient' (the weight with the largest
    # positive coefficient on time_left_pct). Computing from duration gives
    # select_persona the right signal, so snipe/aggressive can win at endgame.
    total = max(1.0, float(fields["total_duration"]))
    remaining = max(0.0, float(fields["remaining_duration"]))
    time_left_pct = max(0.0, min(1.0, remaining / total))
    phase_ratio = 1.0 - time_left_pct

    # Server-provided values win if present; otherwise use our derived ones.
    recent_bid_rate = float(item_block.get("recent_bid_rate", 0.0))
    snap_phase_ratio = item_block.get("phase_ratio")
    snap_time_left_pct = item_block.get("time_left_pct")

    r = _Req()
    r.item_id = item_key
    r.auction_id = auc_id
    r.bot_user_id = bot_id
    r.antecedent_features = {
        "recent_bid_rate": recent_bid_rate,
        "phase_ratio": float(snap_phase_ratio) if snap_phase_ratio else phase_ratio,
        "time_left_pct": float(snap_time_left_pct) if snap_time_left_pct else time_left_pct,
    }
    r.auction_state = {
        "best_price": best_now,
        "threshold_price": fields["snapshot_threshold"],
    }
    r.min_inc_price = fields["min_allowed"]
    r.max_inc_price = float(
        auc.get("MAX_BID_AMOUNT", item_block.get("MAX_BID_AMOUNT", r.min_inc_price * 1000))
    )
    r.threshold_price = r.auction_state["threshold_price"]
    r.time_left_pct = r.antecedent_features["time_left_pct"]
    r.total_duration = fields["total_duration"]
    r.remaining_duration = fields["remaining_duration"]
    return r


def _default_bot_state(item_block: Dict[str, Any], best_now: float) -> Dict[str, Any]:
    return {
        "persona_current": None,
        "num_bids_placed": item_block.get("bid_count", 0),
        "budget_remaining": item_block.get("target_price", 0) - best_now,
    }


# ---------------------------------------------------------------------------
# Wait / cancel
# ---------------------------------------------------------------------------
def _interruptible_wait(cancel_event: Event, auc_id: str, item_assign_key: str, duration: int) -> bool:
    """Sleep for `duration` seconds, checking cancel_event and bots-disabled
    once per second. Returns True if interrupted, False if the full wait
    completed."""
    for i in range(duration):
        if cancel_event.wait(timeout=1.0):
            logger.info(
                "%s: Wait cancelled at %ds/%ds (external bid or new snapshot)",
                item_assign_key, i, duration,
            )
            return True
        if is_bots_disabled(auc_id):
            logger.info("%s: Bots disabled during wait, stopping", item_assign_key)
            return True
    return False


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def _persist_pre_bid(
    item_assign_key: str, bot_id: str, state: Dict[str, Any],
    decision_delay: int, best_now: float, resulting_price: float,
) -> None:
    st = state.copy()
    st["next_scheduled"] = (datetime.utcnow() + timedelta(seconds=decision_delay + 5)).isoformat() + "Z"
    st["decision_timestamp"] = datetime.utcnow().isoformat() + "Z"
    st["decision_best_price"] = float(best_now)
    st["decision_final_bid"] = float(resulting_price)
    persist_bot_state(item_assign_key, bot_id, st)


def _persist_post_bid(
    item_assign_key: str, bot_id: str, auc_id: str, item_key: Any,
    decision: Dict[str, Any], state: Dict[str, Any],
    resulting_price: float, submit_resp: Dict[str, Any],
) -> None:
    updated = decision.get("updated_state", state)
    updated["num_bids_placed"] = updated.get("num_bids_placed", 0) + 1
    updated["last_action_time"] = datetime.utcnow().isoformat() + "Z"
    updated["next_scheduled"] = None

    if submit_resp.get("status") == "success":
        updated["last_bot_bid_amount"] = float(resulting_price)
        updated["last_seen_external_best"] = float(
            _safe_get_current_best(resulting_price)
        )

    persist_bot_state(item_assign_key, bot_id, updated)


# ---------------------------------------------------------------------------
# Worker entry point
# ---------------------------------------------------------------------------
def process_item(auc_id: str, auc: Dict[str, Any], item_key: Any, cancel_event: Event) -> None:
    """Worker for a single (auction, item)."""
    item_assign_key = f"{auc_id}:{item_key}"
    logger.info("Worker started for %s", item_assign_key)

    try:
        bot_id = _ensure_bot_assigned(item_assign_key)
        if not bot_id:
            return

        logger.info("Using bot_id: %s for %s", bot_id, item_assign_key)

        if is_bots_disabled(auc_id):
            logger.info("Auction %s: bots_disabled -> worker for %s exiting", auc_id, item_assign_key)
            return

        item_block = _fetch_item_block(auc, item_key)
        fields = _extract_snapshot_fields(auc_id, item_block, auc)

        best_now = _safe_get_current_best(fields["snapshot_best"])

        if fields["snapshot_threshold"] and best_now >= fields["snapshot_threshold"]:
            logger.info(
                "Auction %s item %s: best_now %.2f >= threshold %.2f -> worker stopping",
                auc_id, item_key, best_now, fields["snapshot_threshold"],
            )
            return

        r = _build_decide_request(auc_id, item_key, bot_id, auc, item_block, fields, best_now)
        state = get_bot_state(item_assign_key, bot_id) or _default_bot_state(item_block, best_now)

        # step_index MUST change each decision so the seeded RNG returns
        # different delay/amount values.
        r.step_index = int(state.get("num_bids_placed", 0))

        decision = DECISION.decide(r, state)
        decision_delay = int(decision.get("delay_seconds") or 1)

        # Amount policy: always bid exactly one minimum increment above the
        # current best. We ignore decision["bid_amount"] intentionally.
        resulting_price = round(best_now + fields["min_allowed"], 2)

        logger.info(
            "Auction %s item %s: bot %s proposing bid=%.2f (best_now=%.2f, min_inc=%.2f) delay=%ss persona=%s",
            auc_id, item_key, bot_id, resulting_price, best_now,
            fields["min_allowed"], decision_delay, decision.get("persona"),
        )

        _persist_pre_bid(
            item_assign_key, bot_id, state,
            decision_delay, best_now, resulting_price,
        )

        if _interruptible_wait(cancel_event, auc_id, item_assign_key, decision_delay):
            return

        # Final checks post-wait: a cancel may have landed between the sleep
        # loop exiting and this point.
        if cancel_event.is_set():
            logger.info("%s: cancelled post-wait, skipping submit", item_assign_key)
            return
        if is_bots_disabled(auc_id):
            logger.info("%s: bots_disabled post-wait, skipping submit", item_assign_key)
            return

        # Someone may have bid during our wait. Step one increment above the
        # new live best instead of our stale target.
        live_before_submit = _safe_get_current_best(best_now)
        if live_before_submit >= resulting_price:
            resulting_price = round(live_before_submit + fields["min_allowed"], 2)

        if fields["snapshot_threshold"] and resulting_price > fields["snapshot_threshold"]:
            logger.info(
                "Auction %s item %s: next bid %.2f would exceed threshold %.2f -> stopping",
                auc_id, item_key, resulting_price, fields["snapshot_threshold"],
            )
            return

        # Cross-bot coordination: skip if our OTHER bot just bid at or above
        # our target within the recent window.
        recent = get_recent_our_bid(auc_id, item_key)
        if recent:
            age = time.time() - float(recent.get("ts", 0))
            other_bot = recent.get("bot")
            other_amount = float(recent.get("amount", 0))
            if (
                other_bot and other_bot != bot_id
                and age <= OUR_BID_SKIP_WINDOW_SEC
                and other_amount >= resulting_price
            ):
                logger.info(
                    "%s: skipping submit; our other bot %s bid %.2f %.2fs ago",
                    item_assign_key, other_bot, other_amount, age,
                )
                return

        # Cross-bot coordination: serialize submits per (auction, item).
        if not try_acquire_bid_mutex(auc_id, item_key, bot_id):
            logger.info(
                "%s: bid mutex busy (our other bot is submitting); skipping",
                item_assign_key,
            )
            return

        try:
            submit_resp = submit_bid(
                auction_id=auc_id, item_id=item_key,
                bid_amount=resulting_price, user_id=bot_id,
            )
            logger.info("Submit bid response (auction %s item %s): %s", auc_id, item_key, submit_resp)

            if submit_resp.get("status") == "success":
                mark_our_bid(auc_id, item_key, bot_id, resulting_price)
        finally:
            release_bid_mutex(auc_id, item_key, bot_id)

        _persist_post_bid(
            item_assign_key, bot_id, auc_id, item_key,
            decision, state, resulting_price, submit_resp,
        )

        latest_best = _safe_get_current_best(resulting_price)
        if fields["snapshot_threshold"] and latest_best >= fields["snapshot_threshold"]:
            logger.info(
                "Auction %s item %s: reached threshold (best %.2f >= threshold %.2f) -> stopping worker",
                auc_id, item_key, latest_best, fields["snapshot_threshold"],
            )
            return

        # We intentionally do NOT self-reschedule. The next snapshot from the
        # WS will drive the next cycle.
        logger.debug(
            "%s: worker completed one bid cycle, waiting for next snapshot",
            item_assign_key,
        )

    except Exception:
        logger.exception("Error in worker for %s - exiting", item_assign_key)


def _ensure_bot_assigned(item_assign_key: str):
    """Return the bot_id for this item.

    Under bot_runner (multi-bot deployment), BOT_ID_OVERRIDE pins the
    process to one bot; that value is returned directly. No Redis hash
    update because two bot processes in the same auction can't share a
    single 'assigned_bot' slot.

    Under the legacy single-bot flow, fall back to a random pick from
    bot_user_ids and record it in Redis for visibility.
    """
    if BOT_ID_OVERRIDE:
        return BOT_ID_OVERRIDE

    if not is_bot_assigned(item_assign_key):
        bot = random.choice(bot_user_ids)
        assign_bot(item_assign_key, str(bot))
        logger.info("Assigned bot %s to item %s", bot, item_assign_key)

    bot_id = assigned_bot_for(item_assign_key)
    if not bot_id:
        logger.warning("No bot assigned for %s - aborting worker", item_assign_key)
    return bot_id


# ---------------------------------------------------------------------------
# Snapshot dispatcher
# ---------------------------------------------------------------------------
def process_snapshot(snapshot: Dict[str, Any]) -> None:
    """Dispatch each (auction, item) in a snapshot to a fresh worker thread.

    Any previous worker for the same item is cancelled first so there is at
    most one active worker per item at a time (modulo the tiny window between
    the cancel and the submit inside the old worker).
    """
    logger.debug("process_snapshot called with snapshot keys: %s", list(snapshot.keys()))

    for auc_id, auc in snapshot.items():
        if not auc_id:
            logger.warning("Received snapshot with empty/invalid auction id — skipping snapshot: %s", auc)
            continue

        if is_bots_disabled(auc_id):
            logger.info(
                "Auction %s: bots_disabled -> skipping (auc_id=%r type=%s). "
                "If this is unexpected, run: python scripts/clear_bots_disabled.py --apply",
                auc_id, auc_id, type(auc_id).__name__,
            )
            continue

        item_keys = list((auc.get("AUCTION_ITEMS") or {}).keys())
        if not item_keys:
            logger.debug("Auction %s: no items in snapshot -> skipping", auc_id)
            continue

        for item_key in item_keys:
            _spawn_worker(auc_id, auc, item_key)


def _spawn_worker(auc_id: str, auc: Dict[str, Any], item_key: Any) -> None:
    """Cancel any existing worker for this item and spawn a fresh one."""
    item_assign_key = f"{auc_id}:{item_key}"

    old_timer = worker_timers.get(item_assign_key)
    if old_timer is not None:
        old_timer.cancel()
        logger.info("%s: Cancelled existing timer", item_assign_key)

    old_event = worker_cancel_events.get(item_assign_key)
    if old_event is not None:
        old_event.set()

    cancel_event = Event()
    worker_cancel_events[item_assign_key] = cancel_event

    logger.info("Spawning worker for %s", item_assign_key)
    t = threading.Thread(
        target=process_item,
        args=(auc_id, auc, item_key, cancel_event),
        daemon=True,
    )
    t.start()
