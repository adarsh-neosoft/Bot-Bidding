"""Schedule joins for auctions listed in TRAN_AUCTION.

For each auction whose window currently includes `now`, spawn one child
process per bot in WS_BOTS. Each child imports ws_adapter and calls
start_ws_adapter for that auction. Children are spawned on a 15s stagger so
they don't all hammer the server simultaneously.

Usage:
    python join_scheduler.py
"""

import logging
import multiprocessing
import threading
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# TODO: move credentials to environment variables / .env
DB_DSN = {
    "dbname":   "CFS_25-12",
    "user":     "postgres",
    "password": "1234",
    "host":     "localhost",
    "port":     "5432",
}

# Bot accounts used to join each auction. Add more rows to run more bots
# per auction. Each entry spawns one OS process.
WS_BOTS = [
    {"username": "TEST_117632", "password": "12345"},
    # {"username": "TEST_117633", "password": "12345"},
]

# Stagger between successive bot spawns (per auction).
BOT_SPAWN_STAGGER_SEC = 15

# If DB datetimes are naive, assume this zone.
ASSUME_DB_TZ = timezone.utc

SELECT_SQL = """
SELECT
    "AUCTION_ID",
    "AUCTION_START_DATE",
    "AUCTION_EXPIRY_DATE",
    "INSPECTION_DATE",
    "AUCTION_VISIBLE_DATE",
    "AUCTION_EXPIRY_LATEST_TIME"
FROM public."TRAN_AUCTION"
ORDER BY "AUCTION_ID" ASC;
"""


#logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
#logger = logging.getLogger("join_scheduler")
from logger_directory import join_scheduler
logger = join_scheduler

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def db_connect():
    return psycopg2.connect(**DB_DSN)


def fetch_auctions():
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SELECT_SQL)
            return cur.fetchall()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Datetime helpers
# ---------------------------------------------------------------------------
def _normalize_dt(dt):
    """Ensure `dt` is tz-aware UTC. None passes through."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ASSUME_DB_TZ)
    return dt.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Child-process entry
# ---------------------------------------------------------------------------
def ws_adapter_target(auc_id, username, password):
    """Runs in the child process; imports ws_adapter lazily so the parent
    stays free of WS side effects."""
    try:
        import ws_adapter
    except Exception:
        logger.exception("Child: failed importing ws_adapter")
        return

    logger.info("Child: starting ws_adapter for auction %s as user %s", auc_id, username)
    try:
        ws_adapter.start_ws_adapter(username, password, auction_pk=str(auc_id))
    except Exception:
        logger.exception("Child: ws_adapter.start_ws_adapter raised")


def spawn_ws_adapter_process(auction_id_str, username, password):
    """Spawn a dedicated OS process for one (auction, bot) pair."""
    ctx = multiprocessing.get_context("spawn")
    p = ctx.Process(
        target=ws_adapter_target,
        args=(auction_id_str, username, password),
        daemon=False,
    )
    p.start()
    logger.info("Spawned subprocess pid=%s for auction %s as user %s", p.pid, auction_id_str, username)
    return p


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
def _is_auction_live(start_dt, expiry_dt, now) -> bool:
    return start_dt is not None and expiry_dt is not None and start_dt <= now <= expiry_dt


def _stagger_spawn(auction_id, bot, delay_seconds):
    """Sleep for `delay_seconds`, then spawn the bot's ws_adapter process."""
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    spawn_ws_adapter_process(auction_id, bot["username"], bot["password"])


def schedule_join(auction_id, start_dt, expiry_date):
    now = datetime.now(timezone.utc)
    if start_dt is None:
        logger.warning("Auction %s has no start date; skipping", auction_id)
        return

    start_dt = _normalize_dt(start_dt)
    expiry_date = _normalize_dt(expiry_date)

    if not _is_auction_live(start_dt, expiry_date, now):
        return

    logger.info("Auction %s is live -> assigning %d bots", auction_id, len(WS_BOTS))

    for i, bot in enumerate(WS_BOTS):
        delay = i * BOT_SPAWN_STAGGER_SEC
        threading.Thread(
            target=_stagger_spawn,
            args=(auction_id, bot, delay),
            daemon=True,
        ).start()
        logger.info("Scheduled bot %s to join in %ds", bot["username"], delay)


def main():
    try:
        rows = fetch_auctions()
    except Exception:
        logger.exception("Failed to fetch auctions from DB")
        return

    if not rows:
        logger.info("No auctions found in TRAN_AUCTION")
        return

    logger.info("Found %d auctions; scheduling joins...", len(rows))
    for r in rows:
        auc_id = r.get("AUCTION_ID")
        start_dt = r.get("AUCTION_START_DATE")
        expiry_date = r.get("AUCTION_EXPIRY_LATEST_TIME")
        if expiry_date is None:
            continue

        logger.debug(
            "Auction %s: start=%s expiry=%s inspection=%s visible=%s",
            auc_id, start_dt, expiry_date,
            r.get("INSPECTION_DATE"), r.get("AUCTION_VISIBLE_DATE"),
        )
        schedule_join(auc_id, start_dt, expiry_date)

    logger.info("Scheduler configured. Waiting for scheduled joins. Press Ctrl-C to stop.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Scheduler shutting down (Ctrl-C)")


if __name__ == "__main__":
    main()
