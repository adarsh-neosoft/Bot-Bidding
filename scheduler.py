"""Scheduler service.

Periodically scans the TRAN_AUCTION table for currently live auctions. For
each live auction, for each bot in BOTS_PER_AUCTION, pushes a job onto the
`stream:auction-jobs` Redis Stream, and refreshes a `live:<auction_id>`
marker so runners can detect auction end.

The scheduler is STATELESS across polls: it does not track what it has
already emitted. Deduplication is the runner's responsibility (via the
lease mechanism), and auction end is inferred by the `live:<aid>` marker
expiring. This keeps restart semantics trivial.

Run as: `python -m scheduler` or under systemd.
"""

import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List

import psycopg2
import psycopg2.extras

from config import (
    BOTS_PER_AUCTION,
    DB_DSN,
    JOB_STREAM,
    JOB_STREAM_MAXLEN,
    LIVE_MARKER_TTL_SEC,
    SCHEDULER_POLL_SEC,
)
from state_store import (
    get_redis,
    set_auction_live_marker,
    set_auction_times,
)


from logger_directory import scheduler
logger = scheduler


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ASSUME_DB_TZ = timezone.utc

SELECT_LIVE_SQL = """
SELECT
    "AUCTION_ID",
    "AUCTION_START_DATE",
    "AUCTION_VISIBLE_DATE",
    "AUCTION_EXPIRY_LATEST_TIME"
FROM "public"."TRAN_AUCTION"
WHERE "AUCTION_START_DATE" <= NOW()
  AND "AUCTION_EXPIRY_LATEST_TIME" >= NOW()
  and is_approved = true
ORDER BY "AUCTION_ID" ASC;
"""


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_shutdown = threading.Event()


def _install_signal_handlers() -> None:
    def _handler(signum, frame):
        logger.info("Received signal %s; shutting down", signum)
        _shutdown.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


# ---------------------------------------------------------------------------
# DB access
# ---------------------------------------------------------------------------
def fetch_live_auctions() -> List[Dict]:
    conn = psycopg2.connect(**DB_DSN)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SELECT_LIVE_SQL)
            return cur.fetchall()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Live-marker TTL: pick something long enough that a missed poll doesn't
# prematurely expire, but short enough that an auction ending between polls
# is noticed within ~LIVE_MARKER_TTL_SEC.
# ---------------------------------------------------------------------------
def _to_epoch_utc(dt) -> float:
    """Convert a DB datetime (possibly naive) into epoch seconds (UTC)."""
    if dt is None:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ASSUME_DB_TZ)
    return dt.astimezone(timezone.utc).timestamp()


def _seconds_remaining(expiry_dt) -> int:
    if expiry_dt is None:
        return 0
    if expiry_dt.tzinfo is None:
        expiry_dt = expiry_dt.replace(tzinfo=ASSUME_DB_TZ)
    now = datetime.now(timezone.utc)
    return max(0, int((expiry_dt - now).total_seconds()))


# ---------------------------------------------------------------------------
# Scan + emit
# ---------------------------------------------------------------------------
def scan_and_emit(r) -> None:
    """Fetch live auctions, emit one job per (auction, bot), refresh markers."""
    try:
        rows = fetch_live_auctions()
    except Exception:
        logger.exception("fetch_live_auctions failed")
        return
    if not rows:
        logger.info("No live auctions found")
        return

    if not BOTS_PER_AUCTION:
        logger.warning("BOTS_PER_AUCTION is empty; nothing to emit")
        return

    emitted = 0
    for row in rows:
        auction_id = str(row["AUCTION_ID"])
        expiry = row.get("AUCTION_EXPIRY_LATEST_TIME")
        seconds_left = _seconds_remaining(expiry)

        # Persist auction start/expiry so child processes can compute
        # remaining/total duration without DB access. "Start" prefers
        # AUCTION_VISIBLE_DATE (when bidding opens) and falls back to
        # AUCTION_START_DATE; "expiry" uses AUCTION_EXPIRY_LATEST_TIME.
        start_dt = row.get("AUCTION_VISIBLE_DATE") or row.get("AUCTION_START_DATE")
        start_ts = _to_epoch_utc(start_dt)
        expiry_ts = _to_epoch_utc(expiry)
        if start_ts and expiry_ts and expiry_ts > start_ts:
            set_auction_times(auction_id, start_ts, expiry_ts)

        # Refresh the live marker. Use min of (auction_remaining_plus_buffer,
        # configured TTL) so the marker disappears shortly after auction end.
        live_ttl = min(LIVE_MARKER_TTL_SEC, max(30, seconds_left + 30))
        set_auction_live_marker(auction_id, live_ttl)

        for bot in BOTS_PER_AUCTION:
            try:
                r.xadd(
                    JOB_STREAM,
                    {"auction_id": auction_id, "bot": bot},
                    maxlen=JOB_STREAM_MAXLEN,
                    approximate=True,
                )
                emitted += 1
            except Exception:
                logger.exception(
                    "Failed to emit job for auction=%s bot=%s", auction_id, bot
                )

    logger.info(
        "Scan complete: auctions=%d bots=%d emitted=%d",
        len(rows), len(BOTS_PER_AUCTION), emitted,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    _install_signal_handlers()

    r = get_redis()
    if r is None:
        logger.error(
            "Redis is required for the scheduler. Configure REDIS_URL and ensure "
            "Redis is reachable before starting."
        )
        sys.exit(1)

    if not BOTS_PER_AUCTION:
        logger.error(
            "BOTS_PER_AUCTION is empty. Set it in .env (e.g., "
            "BOTS_PER_AUCTION=TEST_117632,TEST_117633)."
        )
        sys.exit(1)

    logger.info(
        "Scheduler started. poll=%ds bots_per_auction=%s stream=%s",
        SCHEDULER_POLL_SEC, BOTS_PER_AUCTION, JOB_STREAM,
    )

    # Initial scan immediately, then on poll interval.
    while not _shutdown.is_set():
        scan_and_emit(r)
        _shutdown.wait(SCHEDULER_POLL_SEC)

    logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
