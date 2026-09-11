"""Application configuration.

All values are env-driven via python-dotenv. Secrets (DB password, bot
credentials) must NOT have sensible defaults — production startup should
fail loudly if they're missing.
"""

import os
from typing import Dict, List, Tuple

from dotenv import load_dotenv

load_dotenv()

# Redis connection string. Required in production; state_store will warn
# and fall back to in-memory if unreachable.
REDIS_URL = os.getenv("REDIS_URL")

# Legacy pool (used only by AuctionPlayer's random-assignment fallback).
# The new scheduler/runner stack uses BOT_ACCOUNTS below instead.
_bot_csv = os.getenv("BOT_USER_IDS")
bot_user_ids: List[str] = [i.strip() for i in _bot_csv.split(",") if i.strip()]


# ---------------------------------------------------------------------------
# Scheduler / runner config
# ---------------------------------------------------------------------------
# List of bot usernames that should bid on every live auction. Parsed from
# a CSV env var (e.g., "TEST_117632,TEST_117633"). Order is stable so the
# scheduler emits jobs in a consistent order.
_bots_csv = os.getenv("BOTS_PER_AUCTION")
BOTS_PER_AUCTION: List[str] = [b.strip() for b in _bots_csv.split(",") if b.strip()]

# Credentials map: username -> password, parsed from
# BOT_CREDENTIALS="user1:pass1,user2:pass2"
def _parse_credentials(raw: str) -> Dict[str, str]:
    creds: Dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair:
            continue
        if ":" not in pair:
            raise ValueError(f"BOT_CREDENTIALS entry missing ':' separator: {pair!r}")
        user, pwd = pair.split(":", 1)
        creds[user.strip()] = pwd

    return creds

BOT_CREDENTIALS: Dict[str, str] = _parse_credentials(os.getenv("BOT_CREDENTIALS"))

# Scheduler tuning.
SCHEDULER_POLL_SEC = int(os.getenv("SCHEDULER_POLL_SEC"))
LIVE_MARKER_TTL_SEC = int(os.getenv("LIVE_MARKER_TTL_SEC"))

# Runner lease: claims an (auction, bot) pair exclusively. Must be
# longer than LEASE_RENEWAL_SEC; if a runner dies, its leases expire
# within LEASE_TTL_SEC and another runner can pick them up.
LEASE_TTL_SEC = int(os.getenv("LEASE_TTL_SEC"))
LEASE_RENEWAL_SEC = int(os.getenv("LEASE_RENEWAL_SEC"))

# How often the runner checks if a child is still alive and whether its
# auction is still live (by reading live:<aid> in Redis).
RUNNER_MONITOR_SEC = int(os.getenv("RUNNER_MONITOR_SEC"))

# Stream / consumer group.
JOB_STREAM = os.getenv("JOB_STREAM")
JOB_CONSUMER_GROUP = os.getenv("JOB_CONSUMER_GROUP")
JOB_STREAM_MAXLEN = int(os.getenv("JOB_STREAM_MAXLEN"))


# ---------------------------------------------------------------------------
# Bid coordination across our own bots
# ---------------------------------------------------------------------------
# Cross-bot submission mutex TTL (seconds). Held only during submit.
BID_MUTEX_TTL_SEC = int(os.getenv("BID_MUTEX_TTL_SEC"))

# If one of OUR bots bid within this window, the other bot skips its own
# submit for this snapshot cycle (avoids duplicate bids at the same price).
OUR_BID_SKIP_WINDOW_SEC = int(os.getenv("OUR_BID_SKIP_WINDOW_SEC"))
OUR_BID_RECORD_TTL_SEC = int(os.getenv("OUR_BID_RECORD_TTL_SEC"))


# ---------------------------------------------------------------------------
# Database DSN (scheduler reads TRAN_AUCTION)
# ---------------------------------------------------------------------------
DB_DSN = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}


# ---------------------------------------------------------------------------
# Persona selection config (unchanged)
# ---------------------------------------------------------------------------
PERSONAS = ["patient", "aggressive", "bluffer", "snipe"]

WEIGHT_MATRIX: Dict[str, Dict[str, float]] = {
    "patient": {"phase_ratio": 0.1, "time_left_pct":  0.6, "recent_bid_rate": -0.2, "last_bid_secs":  0.5, "leader_dominance": -0.3, "volatility": 0.0},
    "aggressive": {"phase_ratio": 0.3, "time_left_pct":  0.2, "recent_bid_rate":  0.5, "last_bid_secs": -0.2, "leader_dominance":  0.1, "volatility": 0.1},
    "bluffer": {"phase_ratio": 0.4, "time_left_pct":  0.1, "recent_bid_rate":  0.3, "last_bid_secs":  0.3, "leader_dominance":  0.2, "volatility": 0.5},
    "snipe": {"phase_ratio": 0.7, "time_left_pct": -0.7, "recent_bid_rate": -0.4, "last_bid_secs":  0.3, "leader_dominance": -0.1, "volatility": 0.0},
}
