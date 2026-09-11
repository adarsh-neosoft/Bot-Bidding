"""State store for bot assignments, auction flags, and cross-bot coordination.

Backends:
  * Redis (preferred, cross-process): enabled when REDIS_URL is set and
    reachable at import time. Required for the scheduler/runner stack.
  * In-memory (fallback): single-process only. Useful for local dev.

Redis key schema:

  auction:<auction_id>                  HASH   { assigned_bot, bots_disabled, expiry_override }
  auction:<auction_id>:bot:<bot_id>     STRING JSON-serialized bot state
  auctions                              SET    { auction_id, ... }

  live:<auction_id>                     STRING "1" with TTL = auction remaining
  lease:<auction_id>:<bot>              STRING = runner_id, TTL LEASE_TTL_SEC
  lock:bid:<auction_id>:<item_id>       STRING = bot_id, TTL BID_MUTEX_TTL_SEC
  our_last_bid:<auction_id>:<item_id>   STRING JSON {bot, amount, ts}, TTL OUR_BID_RECORD_TTL_SEC
"""

import json
import logging
import threading
import time
from typing import Any, Dict, Optional

import redis

from config import (
    BID_MUTEX_TTL_SEC,
    OUR_BID_RECORD_TTL_SEC,
    REDIS_URL,
)

from logger_directory import state_store
logger = state_store

# ---------------------------------------------------------------------------
# Backend selection
# ---------------------------------------------------------------------------
_redis: Optional[redis.Redis] = None
_use_redis = False

def _build_redis() -> Optional[redis.Redis]:
    """Create a Redis client with robust timeout / retry settings.

    * socket_timeout  – prevents the consumer from hanging forever on a
      stale connection.  15 s gives xreadgroup (block=5 s) plenty of
      headroom.
    * socket_connect_timeout – fail fast if Redis isn't reachable.
    * retry_on_timeout – automatically retry the command once on timeout
      before raising, which handles transient network blips.
    * max_connections – limit the pool so a flood of threads doesn't
      exhaust Redis connections.
    """
    pool = redis.ConnectionPool.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_timeout=15,
        socket_connect_timeout=5,
        retry_on_timeout=True,
        max_connections=20,
    )
    return redis.Redis(connection_pool=pool)


try:
    _redis = _build_redis()
    _redis.ping()
    _use_redis = True
    logger.info("state_store: using Redis at %s", REDIS_URL)
except Exception as e:
    _redis = None
    _use_redis = False
    logger.warning(
        "state_store: Redis unreachable (%s) — falling back to in-memory. "
        "Cross-process coordination WILL NOT WORK.",
        e,
    )


def get_redis() -> Optional[redis.Redis]:
    """Return the live Redis client if available, else None.

    Used by scheduler/bot_runner which require a real Redis (they refuse to
    start on the in-memory fallback).
    """
    return _redis if _use_redis else None


# ---------------------------------------------------------------------------
# In-memory backing stores (fallback only)
# ---------------------------------------------------------------------------
_mem_hashes: Dict[str, Dict[str, str]] = {}
_mem_strings: Dict[str, str] = {}
_mem_sets: Dict[str, set] = {}
_mem_ttls: Dict[str, float] = {}  # key -> expiry epoch
_inmem_lock = threading.Lock()


def _inmem_is_expired(key: str) -> bool:
    ts = _mem_ttls.get(key)
    return ts is not None and ts <= time.time()


def _inmem_expire_maybe(key: str) -> None:
    if _inmem_is_expired(key):
        _mem_strings.pop(key, None)
        _mem_ttls.pop(key, None)


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------
def _auction_key(auction_id: str) -> str:
    return f"auction:{auction_id}"


def _bot_state_key(auction_id: str, bot_id: str) -> str:
    return f"{_auction_key(auction_id)}:bot:{bot_id}"


def _live_key(auction_id: str) -> str:
    return f"live:{auction_id}"


def _lease_key(auction_id: str, bot_id: str) -> str:
    return f"lease:{auction_id}:{bot_id}"


def _bid_mutex_key(auction_id: str, item_id: Any) -> str:
    return f"lock:bid:{auction_id}:{item_id}"


def _our_last_bid_key(auction_id: str, item_id: Any) -> str:
    return f"our_last_bid:{auction_id}:{item_id}"


def _default_bot_state() -> Dict[str, Any]:
    return {
        "persona_current": None,
        "num_bids_placed": 0,
        "last_action_time": None,
        "budget_remaining": 100000,
    }


# ===========================================================================
# Bot assignment / state
# ===========================================================================
def assign_bot(auction_id: str, bot_id: str) -> None:
    """Record that bot_id is active on auction_id. Initializes a default bot
    state record if none exists yet."""
    akey = _auction_key(auction_id)
    bkey = _bot_state_key(auction_id, bot_id)

    if _use_redis:
        pipe = _redis.pipeline()
        pipe.hset(akey, "assigned_bot", bot_id)
        pipe.sadd("auctions", auction_id)
        if not _redis.exists(bkey):
            pipe.set(bkey, json.dumps(_default_bot_state()))
        pipe.execute()
        return

    with _inmem_lock:
        _mem_hashes.setdefault(akey, {})["assigned_bot"] = bot_id
        _mem_sets.setdefault("auctions", set()).add(auction_id)
        if bkey not in _mem_strings:
            _mem_strings[bkey] = json.dumps(_default_bot_state())


def get_bot_state(auction_id: str, bot_id: str) -> Optional[Dict[str, Any]]:
    bkey = _bot_state_key(auction_id, bot_id)
    if _use_redis:
        val = _redis.get(bkey)
    else:
        with _inmem_lock:
            val = _mem_strings.get(bkey)
    if not val:
        return None
    try:
        return json.loads(val)
    except Exception:
        return None


def persist_bot_state(auction_id: str, bot_id: str, state: Dict[str, Any]) -> None:
    bkey = _bot_state_key(auction_id, bot_id)
    val = json.dumps(state)
    if _use_redis:
        _redis.set(bkey, val)
        return
    with _inmem_lock:
        _mem_strings[bkey] = val


def is_bot_assigned(auction_id: str) -> bool:
    akey = _auction_key(auction_id)
    if _use_redis:
        return _redis.hexists(akey, "assigned_bot")
    with _inmem_lock:
        return "assigned_bot" in _mem_hashes.get(akey, {})


def assigned_bot_for(auction_id: str) -> Optional[str]:
    akey = _auction_key(auction_id)
    if _use_redis:
        v = _redis.hget(akey, "assigned_bot")
    else:
        with _inmem_lock:
            v = _mem_hashes.get(akey, {}).get("assigned_bot")
    return v or None


# ===========================================================================
# Auction-level flags
# ===========================================================================
def _set_auction_field(auction_id: str, field: str, value: str) -> None:
    akey = _auction_key(auction_id)
    if _use_redis:
        _redis.hset(akey, field, value)
        _redis.sadd("auctions", auction_id)
        return
    with _inmem_lock:
        _mem_hashes.setdefault(akey, {})[field] = value
        _mem_sets.setdefault("auctions", set()).add(auction_id)


def _get_auction_field(auction_id: str, field: str) -> Optional[str]:
    akey = _auction_key(auction_id)
    if _use_redis:
        return _redis.hget(akey, field)
    with _inmem_lock:
        return _mem_hashes.get(akey, {}).get(field)


def is_bots_disabled(auction_id: str) -> bool:
    return _get_auction_field(auction_id, "bots_disabled") == "1"


# ===========================================================================
# Live-auction marker (set/refreshed by scheduler)
# ===========================================================================
def set_auction_times(auction_id: str, start_ts: float, expiry_ts: float) -> None:
    """Persist auction start/expiry epoch seconds on the auction hash.

    Written by the scheduler so that subprocesses (which don't have DB
    access) can compute remaining/total duration deterministically.
    """
    akey = _auction_key(auction_id)
    if _use_redis:
        pipe = _redis.pipeline()
        pipe.hset(akey, "start_ts", str(float(start_ts)))
        pipe.hset(akey, "expiry_ts", str(float(expiry_ts)))
        pipe.sadd("auctions", auction_id)
        pipe.execute()
        return
    with _inmem_lock:
        entry = _mem_hashes.setdefault(akey, {})
        entry["start_ts"] = str(float(start_ts))
        entry["expiry_ts"] = str(float(expiry_ts))
        _mem_sets.setdefault("auctions", set()).add(auction_id)


def get_auction_times(auction_id: str) -> Optional[Dict[str, float]]:
    """Return {'start_ts': ..., 'expiry_ts': ...} epoch seconds, or None."""
    akey = _auction_key(auction_id)
    if _use_redis:
        raw = _redis.hmget(akey, "start_ts", "expiry_ts")
        start_raw, exp_raw = raw[0], raw[1]
    else:
        with _inmem_lock:
            h = _mem_hashes.get(akey, {})
            start_raw, exp_raw = h.get("start_ts"), h.get("expiry_ts")
    if not start_raw or not exp_raw:
        return None
    try:
        return {"start_ts": float(start_raw), "expiry_ts": float(exp_raw)}
    except Exception:
        return None


def set_auction_live_marker(auction_id: str, ttl_sec: int) -> None:
    """Mark this auction as still live. Runners poll this to detect end."""
    key = _live_key(auction_id)
    if _use_redis:
        _redis.set(key, "1", ex=max(1, int(ttl_sec)))
        return
    with _inmem_lock:
        _mem_strings[key] = "1"
        _mem_ttls[key] = time.time() + max(1, int(ttl_sec))


def is_auction_live_marker(auction_id: str) -> bool:
    key = _live_key(auction_id)
    if _use_redis:
        return _redis.exists(key) == 1
    with _inmem_lock:
        _inmem_expire_maybe(key)
        return key in _mem_strings


# ===========================================================================
# Runner lease (claim exclusive ownership of an (auction, bot) pair)
# ===========================================================================
def try_acquire_lease(auction_id: str, bot_id: str, runner_id: str, ttl_sec: int) -> bool:
    """Set the lease key only if it doesn't already exist. Returns True on win."""
    key = _lease_key(auction_id, bot_id)
    if _use_redis:
        return bool(_redis.set(key, runner_id, nx=True, ex=max(1, int(ttl_sec))))
    with _inmem_lock:
        _inmem_expire_maybe(key)
        if key in _mem_strings:
            return False
        _mem_strings[key] = runner_id
        _mem_ttls[key] = time.time() + max(1, int(ttl_sec))
        return True


def renew_lease(auction_id: str, bot_id: str, runner_id: str, ttl_sec: int) -> bool:
    """Refresh the lease TTL iff we still own it. Returns True on success."""
    key = _lease_key(auction_id, bot_id)
    if _use_redis:
        # Compare-and-extend via Lua: only extend if value matches runner_id.
        lua = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "  return redis.call('PEXPIRE', KEYS[1], ARGV[2]) "
            "else return 0 end"
        )
        res = _redis.eval(lua, 1, key, runner_id, int(ttl_sec) * 1000)
        return bool(res)
    with _inmem_lock:
        _inmem_expire_maybe(key)
        if _mem_strings.get(key) != runner_id:
            return False
        _mem_ttls[key] = time.time() + max(1, int(ttl_sec))
        return True


def release_lease(auction_id: str, bot_id: str, runner_id: str) -> None:
    """Release lease only if we still own it (idempotent)."""
    key = _lease_key(auction_id, bot_id)
    if _use_redis:
        lua = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "  return redis.call('DEL', KEYS[1]) "
            "else return 0 end"
        )
        _redis.eval(lua, 1, key, runner_id)
        return
    with _inmem_lock:
        if _mem_strings.get(key) == runner_id:
            _mem_strings.pop(key, None)
            _mem_ttls.pop(key, None)


# ===========================================================================
# Cross-bot bid coordination
# ===========================================================================
def try_acquire_bid_mutex(auction_id: str, item_id: Any, bot_id: str) -> bool:
    """Short-lived lock held during submit_bid so two of OUR bots don't
    submit identical prices in the same millisecond."""
    key = _bid_mutex_key(auction_id, item_id)
    if _use_redis:
        return bool(_redis.set(key, bot_id, nx=True, ex=max(1, BID_MUTEX_TTL_SEC)))
    with _inmem_lock:
        _inmem_expire_maybe(key)
        if key in _mem_strings:
            return False
        _mem_strings[key] = bot_id
        _mem_ttls[key] = time.time() + max(1, BID_MUTEX_TTL_SEC)
        return True


def release_bid_mutex(auction_id: str, item_id: Any, bot_id: str) -> None:
    key = _bid_mutex_key(auction_id, item_id)
    if _use_redis:
        lua = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "  return redis.call('DEL', KEYS[1]) "
            "else return 0 end"
        )
        _redis.eval(lua, 1, key, bot_id)
        return
    with _inmem_lock:
        if _mem_strings.get(key) == bot_id:
            _mem_strings.pop(key, None)
            _mem_ttls.pop(key, None)


def mark_our_bid(auction_id: str, item_id: Any, bot_id: str, amount: float) -> None:
    """Record that one of OUR bots just submitted a bid so the other bot
    can skip if it would be a duplicate within OUR_BID_SKIP_WINDOW_SEC."""
    key = _our_last_bid_key(auction_id, item_id)
    payload = json.dumps({
        "bot": bot_id,
        "amount": float(amount),
        "ts": time.time(),
    })
    if _use_redis:
        _redis.set(key, payload, ex=max(1, OUR_BID_RECORD_TTL_SEC))
        return
    with _inmem_lock:
        _mem_strings[key] = payload
        _mem_ttls[key] = time.time() + max(1, OUR_BID_RECORD_TTL_SEC)


def get_recent_our_bid(auction_id: str, item_id: Any) -> Optional[Dict[str, Any]]:
    """Return the most recent in-window bid record for this item, or None."""
    key = _our_last_bid_key(auction_id, item_id)
    if _use_redis:
        val = _redis.get(key)
    else:
        with _inmem_lock:
            _inmem_expire_maybe(key)
            val = _mem_strings.get(key)
    if not val:
        return None
    try:
        return json.loads(val)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Shared worker registries used by AuctionPlayer (process-local).
# ---------------------------------------------------------------------------
worker_timers: Dict[str, threading.Timer] = {}
worker_cancel_events: Dict[str, threading.Event] = {}
