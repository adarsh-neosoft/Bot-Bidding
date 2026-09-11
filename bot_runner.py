"""Bot runner service.

Consumes jobs from `stream:auction-jobs` (Redis Stream, consumer group
`runners`). For each claimable (auction, bot) pair:

  1. Attempt `try_acquire_lease` — compare-and-set a Redis key that prevents
     two runners from running the same pair.
  2. On lease win, spawn a subprocess running `ws_adapter.start_ws_adapter`
     with a stop_event.
  3. Renew the lease every LEASE_RENEWAL_SEC.
  4. Periodically check `live:<auction_id>`; when gone, signal the child to
     stop, wait, release the lease.

If the runner dies mid-auction, its leases expire (LEASE_TTL_SEC). The
scheduler's next scan re-emits jobs for those auctions; any surviving
runner picks them up. Total recovery window ~ LEASE_TTL_SEC + SCHEDULER_POLL_SEC.

Run as: `python -m bot_runner` or under systemd.
"""

import logging
import multiprocessing
import os
import signal
import socket
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Optional

from config import (
    BOT_CREDENTIALS,
    JOB_CONSUMER_GROUP,
    JOB_STREAM,
    LEASE_RENEWAL_SEC,
    LEASE_TTL_SEC,
    RUNNER_MONITOR_SEC,
)
from state_store import (
    get_redis,
    is_auction_live_marker,
    release_lease,
    renew_lease,
    try_acquire_lease,
)


from logger_directory import bot_runner
logger = bot_runner

# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------
def _gen_runner_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


RUNNER_ID = _gen_runner_id()


# ---------------------------------------------------------------------------
# Active jobs
# ---------------------------------------------------------------------------
@dataclass
class ActiveJob:
    auction_id: str
    bot: str
    process: multiprocessing.Process
    stop_event: "multiprocessing.Event"
    msg_id: str  # stream message id, used for XACK


_active: Dict[str, ActiveJob] = {}  # key = f"{auction_id}:{bot}"
_active_lock = threading.Lock()


def _key(auction_id: str, bot: str) -> str:
    return f"{auction_id}:{bot}"


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
# Subprocess entry (runs in child process)
# ---------------------------------------------------------------------------
def _child_entry(auction_id: str, username: str, password: str,
                 stop_event: "multiprocessing.Event") -> None:
    """Entry point for the spawned bot subprocess.

    Imports ws_adapter lazily so the parent runner stays free of WS side
    effects, and sets BOT_USERNAME so AuctionPlayer knows which bot is
    active in this process.
    """
    os.environ["BOT_USERNAME"] = username

    # Re-install default SIGTERM / SIGINT so the parent's signal propagation
    # doesn't raise KeyboardInterrupt at arbitrary moments; we rely on
    # stop_event for clean shutdown.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    try:
        import ws_adapter
    except Exception:
        logger.exception("child: failed importing ws_adapter")
        return

    try:
        ws_adapter.start_ws_adapter(
            username, password,
            auction_pk=auction_id,
            stop_event=stop_event,
        )
    except Exception:
        logger.exception("child: ws_adapter.start_ws_adapter raised")


# ---------------------------------------------------------------------------
# Spawn / terminate helpers
# ---------------------------------------------------------------------------
def _spawn_child(auction_id: str, bot: str) -> Optional[ActiveJob]:
    password = BOT_CREDENTIALS.get(bot)
    if not password:
        logger.error(
            "No credentials for bot=%s in BOT_CREDENTIALS; cannot spawn child",
            bot,
        )
        return None

    ctx = multiprocessing.get_context("spawn")
    stop_event = ctx.Event()
    p = ctx.Process(
        target=_child_entry,
        args=(auction_id, bot, password, stop_event),
        daemon=False,
    )
    p.start()

    logger.info(
        "Spawned child pid=%s auction=%s bot=%s",
        p.pid, auction_id, bot,
    )
    return ActiveJob(
        auction_id=auction_id, bot=bot,
        process=p, stop_event=stop_event,
        msg_id="",  # filled in by caller
    )


def _stop_child(job: ActiveJob, reason: str, grace_sec: int = 10) -> None:
    """Ask the child to stop, wait grace_sec, escalate to terminate/kill."""
    logger.info(
        "Stopping child auction=%s bot=%s reason=%s pid=%s",
        job.auction_id, job.bot, reason, job.process.pid,
    )
    try:
        job.stop_event.set()
    except Exception:
        pass

    job.process.join(timeout=grace_sec)
    if job.process.is_alive():
        logger.warning(
            "Child auction=%s bot=%s still alive after %ds; terminating",
            job.auction_id, job.bot, grace_sec,
        )
        try:
            job.process.terminate()
        except Exception:
            pass
        job.process.join(timeout=5)
    if job.process.is_alive():
        logger.warning(
            "Child auction=%s bot=%s still alive after terminate; killing",
            job.auction_id, job.bot,
        )
        try:
            job.process.kill()
        except Exception:
            pass
        job.process.join(timeout=2)


# ---------------------------------------------------------------------------
# Monitor thread: reap dead children, detect auction end, release leases
# ---------------------------------------------------------------------------
def _monitor_loop(r) -> None:
    while not _shutdown.is_set():
        _shutdown.wait(RUNNER_MONITOR_SEC)
        if _shutdown.is_set():
            break

        to_remove = []
        with _active_lock:
            jobs = list(_active.values())

        for job in jobs:
            k = _key(job.auction_id, job.bot)

            try:
                # Reap crashed/exited children.
                if not job.process.is_alive():
                    logger.warning(
                        "Child exited auction=%s bot=%s exitcode=%s",
                        job.auction_id, job.bot, job.process.exitcode,
                    )
                    release_lease(job.auction_id, job.bot, RUNNER_ID)
                    to_remove.append(k)
                    try:
                        r.xack(JOB_STREAM, JOB_CONSUMER_GROUP, job.msg_id)
                    except Exception:
                        logger.exception("xack failed for %s", job.msg_id)
                    continue

                # Detect auction end via live marker disappearance.
                if not is_auction_live_marker(job.auction_id):
                    _stop_child(job, reason="auction-ended")
                    release_lease(job.auction_id, job.bot, RUNNER_ID)
                    to_remove.append(k)
                    try:
                        r.xack(JOB_STREAM, JOB_CONSUMER_GROUP, job.msg_id)
                    except Exception:
                        logger.exception("xack failed for %s", job.msg_id)
                    continue
            except Exception:
                # A transient Redis error (timeout, connection blip) here
                # must not kill this thread for the rest of the process's
                # life — skip this job for this tick, retry next tick.
                logger.exception(
                    "monitor tick failed for auction=%s bot=%s; will retry next tick",
                    job.auction_id, job.bot,
                )
                continue

        if to_remove:
            with _active_lock:
                for k in to_remove:
                    _active.pop(k, None)


# ---------------------------------------------------------------------------
# Lease renewer thread
# ---------------------------------------------------------------------------
def _renewer_loop() -> None:
    while not _shutdown.is_set():
        _shutdown.wait(LEASE_RENEWAL_SEC)
        if _shutdown.is_set():
            break

        with _active_lock:
            jobs = list(_active.values())
        for job in jobs:
            try:
                ok = renew_lease(job.auction_id, job.bot, RUNNER_ID, LEASE_TTL_SEC)
            except Exception:
                # Transient Redis error (timeout, connection blip): skip this
                # renewal tick rather than killing the renewer thread outright.
                # We'll retry on the next tick, well within LEASE_TTL_SEC.
                logger.exception(
                    "renew_lease failed for auction=%s bot=%s; will retry next tick",
                    job.auction_id, job.bot,
                )
                continue
            if not ok:
                logger.warning(
                    "Lease lost for auction=%s bot=%s; stopping child",
                    job.auction_id, job.bot,
                )
                # Someone else owns it now. Stop our child.
                _stop_child(job, reason="lease-lost")
                with _active_lock:
                    _active.pop(_key(job.auction_id, job.bot), None)


# ---------------------------------------------------------------------------
# Stream consumption
# ---------------------------------------------------------------------------
def _ensure_consumer_group(r) -> None:
    try:
        r.xgroup_create(JOB_STREAM, JOB_CONSUMER_GROUP, id="$", mkstream=True)
        logger.info("Created consumer group %s on %s", JOB_CONSUMER_GROUP, JOB_STREAM)
    except Exception as e:
        # BUSYGROUP Consumer Group name already exists -> fine.
        if "BUSYGROUP" in str(e):
            logger.info("Consumer group %s already exists", JOB_CONSUMER_GROUP)
        else:
            raise


def _handle_job(r, msg_id: str, fields: dict) -> None:
    auction_id = fields.get("auction_id")
    bot = fields.get("bot")
    if not auction_id or not bot:
        logger.warning("Malformed job %s: %s", msg_id, fields)
        r.xack(JOB_STREAM, JOB_CONSUMER_GROUP, msg_id)
        return

    k = _key(auction_id, bot)
    with _active_lock:
        already = k in _active
    if already:
        # Already running on this runner; ack and move on.
        r.xack(JOB_STREAM, JOB_CONSUMER_GROUP, msg_id)
        return

    if not try_acquire_lease(auction_id, bot, RUNNER_ID, LEASE_TTL_SEC):
        logger.debug(
            "Lease busy auction=%s bot=%s (another runner owns it); skipping",
            auction_id, bot,
        )
        r.xack(JOB_STREAM, JOB_CONSUMER_GROUP, msg_id)
        return

    job = _spawn_child(auction_id, bot)
    if job is None:
        # Credentials missing or spawn failed; release lease and ack.
        release_lease(auction_id, bot, RUNNER_ID)
        r.xack(JOB_STREAM, JOB_CONSUMER_GROUP, msg_id)
        return

    job.msg_id = msg_id
    with _active_lock:
        _active[k] = job


def _consume_loop(r) -> None:
    """Block on XREADGROUP; dispatch each claimed job.

    After MAX_CONSECUTIVE_ERRORS consecutive failures the runner tries to
    obtain a fresh Redis connection (in case the old one is poisoned) and
    backs off for BACKOFF_SEC before resuming.
    """
    MAX_CONSECUTIVE_ERRORS = 5
    BACKOFF_SEC = 10
    consecutive_errors = 0

    while not _shutdown.is_set():
        try:
            # Block up to 5 seconds; wake frequently enough to notice shutdown.
            resp = r.xreadgroup(
                JOB_CONSUMER_GROUP, RUNNER_ID,
                {JOB_STREAM: ">"},
                count=10, block=5000,
            )
            consecutive_errors = 0  # success resets the counter
        except Exception:
            consecutive_errors += 1
            logger.exception(
                "xreadgroup failed (%d consecutive); retrying after %ds",
                consecutive_errors,
                1 if consecutive_errors < MAX_CONSECUTIVE_ERRORS else BACKOFF_SEC,
            )
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                # Try a fresh Redis connection in case the old one is dead.
                try:
                    fresh = get_redis()
                    if fresh is not None:
                        fresh.ping()
                        r = fresh
                        logger.info("Obtained fresh Redis connection")
                except Exception:
                    logger.exception("Fresh Redis connection also failed")
                _shutdown.wait(BACKOFF_SEC)
            else:
                _shutdown.wait(1)
            continue

        if not resp:
            continue

        for _stream_name, entries in resp:
            for msg_id, fields in entries:
                if _shutdown.is_set():
                    return
                try:
                    _handle_job(r, msg_id, fields)
                except Exception:
                    logger.exception("Error handling job %s: %s", msg_id, fields)


# ---------------------------------------------------------------------------
# Graceful drain on shutdown
# ---------------------------------------------------------------------------
def _drain_and_shutdown() -> None:
    with _active_lock:
        jobs = list(_active.values())
        _active.clear()

    logger.info("Draining %d active child(ren)", len(jobs))
    for job in jobs:
        _stop_child(job, reason="runner-shutdown")
        release_lease(job.auction_id, job.bot, RUNNER_ID)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    _install_signal_handlers()

    r = get_redis()
    if r is None:
        logger.error("Redis is required for bot_runner. Configure REDIS_URL.")
        sys.exit(1)

    if not BOT_CREDENTIALS:
        logger.error(
            "BOT_CREDENTIALS is empty. Set it in .env "
            "(e.g., BOT_CREDENTIALS=user1:pass1,user2:pass2)"
        )
        sys.exit(1)

    _ensure_consumer_group(r)

    logger.info(
        "Runner started. id=%s stream=%s group=%s lease_ttl=%ds",
        RUNNER_ID, JOB_STREAM, JOB_CONSUMER_GROUP, LEASE_TTL_SEC,
    )

    threading.Thread(target=_renewer_loop, daemon=True, name="lease-renewer").start()
    threading.Thread(target=_monitor_loop, args=(r,), daemon=True, name="monitor").start()

    try:
        _consume_loop(r)
    finally:
        _drain_and_shutdown()
        logger.info("Runner stopped.")


if __name__ == "__main__":
    main()
