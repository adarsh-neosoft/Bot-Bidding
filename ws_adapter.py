"""WebSocket adapter for one bot-per-auction session.

Responsibilities per process:
  * HTTP login to obtain session cookies.
  * Open ONE persistent WebSocket for the auction's lifetime.
  * Receive snapshots, convert them to AuctionPlayer's format, and dispatch.
  * Send bids on the same persistent socket. A request_id -> Event registry
    lets senders wait for the server's echo of their own bid without
    contending with the single _on_message reader.
  * Monkey-patch AucApi.submit_bid (and AuctionPlayer.submit_bid) so the
    rest of the app sends bids over WS without knowing about HTTP.
"""

import os
import json
import re
import threading
import time
import uuid

import requests
import websocket  # pip install websocket-client

import AucApi
import AuctionPlayer
from AuctionPlayer import process_external_snapshot

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HTTP_BASE = os.getenv("HTTP_BASE")
LOGIN_PATH = os.getenv("LOGIN_PATH")
WS_BASE = os.getenv("WS_BASE")
WS_JOIN_PATH = os.getenv("WS_JOIN_PATH")

# Defaults used when running this module as a standalone script.
AUCTION_PK = ""

# Persistent-WS keepalive. ping_interval MUST be greater than ping_timeout.
PERSISTENT_PING_INTERVAL = int(os.getenv("PERSISTENT_PING_INTERVAL"))
PERSISTENT_PING_TIMEOUT = int(os.getenv("PERSISTENT_PING_TIMEOUT"))

# How long a sender will block waiting for the server to echo its own bid
# back on the persistent socket.
PERSISTENT_SUBMIT_TIMEOUT = float(os.getenv("PERSISTENT_SUBMIT_TIMEOUT"))


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
from logger_directory import ws_adapter
logger = ws_adapter


# ---------------------------------------------------------------------------
# Module-level shared state (per bot process)
# ---------------------------------------------------------------------------
_global_ws = None
_global_ws_lock = threading.Lock()        # guards swaps of _global_ws
_global_ws_send_lock = threading.Lock()   # serializes .send() across threads

# Pending bid submissions awaiting the server's echo. Keyed by request_id.
_PENDING_SUBMITS = {}
_PENDING_LOCK = threading.Lock()

# Last-known per-auction item metadata so update_bid messages missing fields
# (min_inc in particular) don't regress to default `1`.
_LAST_ITEM_MAP = {}  # auction_id -> {item_id: item_block}


def _lookup_cached_best(auction_id, item_id):
    """Return the most recently observed BEST_PRICE for (auction_id, item_id),
    or None if nothing has been cached yet. Registered with AuctionPlayer as
    its live-price lookup so submits use the latest price this connection
    has actually seen instead of a stale decision-time snapshot."""
    item = _LAST_ITEM_MAP.get(auction_id, {}).get(item_id)
    if item is None:
        return None
    return item.get("BEST_PRICE")


# ---------------------------------------------------------------------------
# HTTP login
# ---------------------------------------------------------------------------
def login_and_get_session(username: str, password: str) -> requests.Session:
    """POST credentials, validate a token field, return the Session holding
    the session cookie."""
    s = requests.Session()
    login_url = HTTP_BASE + LOGIN_PATH
    r = s.post(
        login_url,
        json={"username": username, "password": password},
        allow_redirects=True,
        timeout=10,
        verify=False
    )
    if r.status_code not in (200,):
        raise RuntimeError(f"Login failed: {r.status_code} {r.text[:400]}")

    #body = r.json()
    #if not body.get("Your Token"):
    #    raise RuntimeError("No session cookie/token received after login")

    logger.info("Login cookies: %s", s.cookies.get_dict())
    logger.info("Logged in successfully, session cookie found.")
    return s


# ---------------------------------------------------------------------------
# Snapshot building helpers
# ---------------------------------------------------------------------------
def _safe_float(value, default=0.0):
    try:
        return float(value) if value is not None else default
    except Exception:
        return default


def _build_item_from_product(product: dict) -> dict:
    """Convert one product dict from the server into our internal item
    block shape."""
    pid = product.get("auction_product_id") or product.get("id")
    raw_min = product.get("minimum_increment_price")
    min_bid = _safe_float(raw_min, default=None) if raw_min is not None else None

    item = {
        "ITEM_ID": pid,
        "BEST_PRICE": _safe_float(product.get("current_bid") or product.get("starting_bid"), 0.0),
        "THRESHOLD_PRICE": _safe_float(product.get("target_price"), 0.0),
        "MIN_BID_AMOUNT": min_bid if min_bid is not None else 1,
        "MAX_BID_AMOUNT": _safe_float(product.get("maximum_increment_price"), 999999999),
        **product,
    }
    return item


def _extract_products_and_auction_id(join_data: dict):
    """Return (products_list, auction_id_guess) from many possible server
    payload shapes. auction_id_guess may be None."""
    auction_id = (
        join_data.get("auction_id")
        or join_data.get("auctionId")
        or join_data.get("auction")
    )
    # 'auction_data.auction_id' fallback (may be absent)
    if not auction_id:
        ad = join_data.get("auction_data") or {}
        if isinstance(ad, dict):
            auction_id = ad.get("auction_id")

    # Try top-level lists first.
    if isinstance(join_data.get("products_data"), (list, tuple)):
        return list(join_data["products_data"]), auction_id

    if isinstance(join_data.get("products"), (list, tuple)):
        return list(join_data["products"]), auction_id

    # auction_data may be a list of dicts, or a dict keyed by auction id,
    # or a dict with its own `products` key.
    ad = join_data.get("auction_data")

    if isinstance(ad, (list, tuple)) and ad:
        for el in ad:
            if isinstance(el, dict) and isinstance(el.get("products"), (list, tuple)):
                return el["products"] or [], auction_id or el.get("auction_id") or el.get("auctionId")
        if isinstance(ad[0], dict):
            return ad[0].get("products") or [], auction_id
        return [], auction_id

    if isinstance(ad, dict):
        for k, v in ad.items():
            if isinstance(v, dict) and isinstance(v.get("products"), (list, tuple)):
                return v["products"] or [], auction_id or k
        if isinstance(ad.get("products"), (list, tuple)):
            return ad["products"], auction_id

    # nested `data` container
    dd = join_data.get("data")
    if isinstance(dd, dict):
        if isinstance(dd.get("products"), (list, tuple)):
            return dd["products"], auction_id
        if isinstance(dd.get("products_data"), (list, tuple)):
            return dd["products_data"], auction_id

    # last-resort scan
    for k, v in join_data.items():
        if k in ("items", "products_list") and isinstance(v, (list, tuple)):
            return list(v), auction_id

    return [], auction_id


def _convert_join_response_to_snapshot(join_data: dict) -> dict:
    """Convert a join_auction server response to an AuctionPlayer snapshot.
    Always returns a dict keyed by one auction id."""
    try:
        products, auction_id = _extract_products_and_auction_id(join_data)

        if not products:
            logger.debug("No products found in join_data; keys=%s", list(join_data.keys()))
            aid = str(auction_id or AUCTION_PK or "unknown_auction")
            return {aid: {"AUCTION_ITEMS": {}}}

        item_map = {}
        for p in products:
            if not isinstance(p, dict):
                continue
            item = _build_item_from_product(p)
            if item["ITEM_ID"] is None:
                continue
            item_map[item["ITEM_ID"]] = item

        auction_key = str(auction_id or AUCTION_PK or "unknown_auction")
        _LAST_ITEM_MAP[auction_key] = {k: v.copy() for k, v in item_map.items()}

        auction_block = {"AUCTION_ITEMS": item_map}
        item_mins = {
            v.get("MIN_BID_AMOUNT") for v in item_map.values()
            if v.get("MIN_BID_AMOUNT") is not None
        }
        if len(item_mins) == 1:
            auction_block["MIN_BID_AMOUNT"] = next(iter(item_mins))
        
        logger.info("Returning Auction Block for Auction Key: %s -- %s", auction_key, auction_block)
        return {auction_key: auction_block}

    except Exception:
        logger.exception("Error converting join response to snapshot")
        return {str(AUCTION_PK or "unknown_auction"): {"AUCTION_ITEMS": {}}}


def _convert_update_to_snapshot(update_msg: dict) -> dict:
    """Convert a place_bid/bid_added update into a single-item snapshot.
    Falls back to the cached metadata for anything the update omits.

    The server uses more than one shape for these messages: some wrap
    fields under 'data' (e.g. {'data': {...}, 'action': 'place_bid', ...})
    while others (e.g. action='bid_added') are flat with product_id/
    bid_amount/auction_id at the top level. We check both so a message's
    real price/id fields aren't silently dropped just because they weren't
    where the original nested shape expected them.
    """
    try:
        data = update_msg.get("data") or {}
        pid = (
            data.get("auction_product")
            or data.get("auction_product_id")
            or update_msg.get("product_id")
            or update_msg.get("auction_product_id")
        )
        if pid is None:
            logger.debug("update missing product id; keys=%s", list(update_msg.keys()))
            return {}

        # NOTE: `pk` on these messages is the bid/record id, not the auction
        # id (confirmed via logs: it echoed as an unrelated small integer
        # while the real auction id is a UUID) - do not use it as a fallback
        # here. Each ws_adapter process is bound to exactly one auction for
        # its lifetime, so AUCTION_PK is the correct fallback, mirroring
        # _convert_join_response_to_snapshot.
        auction_id = (
            update_msg.get("auction_id")
            or data.get("auction_id")
            or AUCTION_PK
        )

        # bot_features may be top-level, nested under 'data', or absent.
        bot_features = (
            update_msg.get("bot_features")
            or data.get("bot_features")
            or data.get("features")
            or {}
        )

        if not bot_features:
            logger.debug(
                "No bot_features in update_msg (keys=%s); using cached metadata",
                list(update_msg.keys()),
            )

        # Use cached metadata as fallback for any missing fields.
        cached = _LAST_ITEM_MAP.get(auction_id, {}).get(pid, {})

        raw_min = bot_features.get("minimum_increment_price")
        if raw_min is None:
            min_bid = cached.get("MIN_BID_AMOUNT", 1)
        else:
            min_bid = float(raw_min)

        # A flat top-level `bid_amount` (as sent on action='bid_added') is
        # the actual new price and takes priority over the stale cache.
        raw_current = (
            bot_features.get("current_bid")
            or update_msg.get("bid_amount")
            or data.get("bid_amount")
            or data.get("current_bid")
            or cached.get("BEST_PRICE")
        )
        raw_target = bot_features.get("target_price") or cached.get("THRESHOLD_PRICE")
        raw_max = bot_features.get("maximum_increment_price") or cached.get("MAX_BID_AMOUNT")

        item_block = {
            "ITEM_ID": pid,
            "BEST_PRICE": float(raw_current) if raw_current is not None else 0.0,
            "THRESHOLD_PRICE": float(raw_target) if raw_target is not None else 0.0,
            "MIN_BID_AMOUNT": min_bid,
            "MAX_BID_AMOUNT": float(raw_max) if raw_max is not None else 999999999,
            **bot_features,  # include any extra fields the server did send
        }

        _LAST_ITEM_MAP.setdefault(auction_id, {})[pid] = item_block.copy()

        return {
            auction_id: {
                "AUCTION_ITEMS": {pid: item_block},
                "MIN_BID_AMOUNT": item_block["MIN_BID_AMOUNT"],
            }
        }
    except Exception:
        logger.exception("Error converting update to snapshot")
        return {}


# ---------------------------------------------------------------------------
# Pending-submit registry
# ---------------------------------------------------------------------------
def _signal_pending(req_id: str, data: dict):
    """If this request_id matches a pending submit, hand it the server
    response, wake the waiter, and return the entry (so the caller can
    recover which auction/item this submit was for)."""
    if not req_id:
        return None
    with _PENDING_LOCK:
        entry = _PENDING_SUBMITS.get(req_id)
    if entry is not None:
        entry["result"] = data
        entry["event"].set()
    return entry


def _signal_pending_unmatched(data: dict):
    """Resolve a response that carries no request_id (the server's
    synchronous place_bid validation errors and its 'bid_added' success
    confirmations both omit it - see e.g. {'status': 'error', 'message':
    'Bid must be greater than current highest bid...'} or {'action':
    'bid_added', 'status': 'success', ...}). Without a request_id we can't
    be certain which submit this belongs to, so only act when exactly one
    is in flight - the common case since a submitter blocks until its own
    echo/timeout before returning. Returns the resolved entry, or None."""
    with _PENDING_LOCK:
        if len(_PENDING_SUBMITS) != 1:
            return None
        req_id, entry = next(iter(_PENDING_SUBMITS.items()))
    entry["result"] = data
    entry["event"].set()
    logger.debug("Resolved request-id-less response against sole pending submit %s", req_id)
    return entry


_REJECTED_PRICE_RE = re.compile(r"([\d,]+\.\d{1,2})")


def _parse_rejected_price(message: str):
    """Pull the real current-highest price out of a rejection message like
    'Bid must be greater than current highest bid (9200.00).' so the local
    cache can be corrected immediately instead of staying stale until the
    next (possibly non-existent) price-bearing broadcast."""
    if not message:
        return None
    matches = _REJECTED_PRICE_RE.findall(message)
    if not matches:
        return None
    try:
        return float(matches[-1].replace(",", ""))
    except ValueError:
        return None


def _update_cached_price(auction_id, product_id, price) -> None:
    if auction_id is None or product_id is None or price is None:
        return
    item = _LAST_ITEM_MAP.setdefault(auction_id, {}).setdefault(product_id, {"ITEM_ID": product_id})
    old = item.get("BEST_PRICE")
    try:
        new_price = float(price)
    except (TypeError, ValueError):
        return
    item["BEST_PRICE"] = new_price
    if old != new_price:
        logger.info(
            "Corrected cached best price for %s:%s: %s -> %s",
            auction_id, product_id, old, new_price,
        )


# ---------------------------------------------------------------------------
# WebSocket callbacks
# ---------------------------------------------------------------------------
def _on_open(ws):
    global _global_ws
    with _global_ws_lock:
        _global_ws = ws
    logger.info("WS open. Sending join_auction for auction=%s", AUCTION_PK)
    payload = {"pk": AUCTION_PK, "action": "join_auction", "request_id": str(uuid.uuid4())}
    try:
        ws.send(json.dumps(payload))
    except Exception:
        logger.exception("Failed to send join payload on open")


def _on_message(ws, message):
    try:
        data = json.loads(message)
    except Exception:
        logger.debug("Non-json WS message: %s", message)
        return

    action = data.get("action") or data.get("type")
    status = data.get("status")

    if status == "success" and action == "join_auction":
        logger.info("Join confirmed for auction: %s", data.get("auction_id"))
        snap = _convert_join_response_to_snapshot(data)
        if snap:
            process_external_snapshot(snap)
        return

    if status == "success" and action != "join_auction":
        # Confirmation of a placed bid. The server uses different action
        # names for this ("place_bid", "bid_update", "bid_added", ...) and
        # none of them reliably echo our request_id, so resolve by id when
        # present and fall back to "the one submit currently in flight"
        # otherwise (see _signal_pending_unmatched).
        entry = _signal_pending(data.get("request_id"), data) or _signal_pending_unmatched(data)

        # Correct the cache from whatever ground-truth price this message
        # carries (flat `bid_amount` on bid_added, or nested under 'data')
        # rather than waiting on a broadcast shape that may never arrive.
        payload = (entry or {}).get("payload") or {}
        msg_data = data.get("data") or {}
        auction_id = data.get("auction_id") or msg_data.get("auction_id") or payload.get("pk")
        product_id = data.get("product_id") or msg_data.get("auction_product") or payload.get("product_id")
        bid_amount = data.get("bid_amount") or msg_data.get("bid_amount")
        if bid_amount is not None:
            _update_cached_price(auction_id, product_id, bid_amount)

        snap = _convert_update_to_snapshot(data)
        if snap:
            process_external_snapshot(snap)
        return

    if status in ("error", "failed"):
        # Some place_bid validation errors come back with neither `action`
        # nor `request_id` (e.g. "Bid must be greater than current highest
        # bid"). Resolve by id when present, else against the sole in-flight
        # submit, instead of letting it time out for 10s.
        if data.get("request_id"):
            entry = _signal_pending(data.get("request_id"), data)
            logger.info("Error response matched by request_id: %s", data)
        else:
            entry = _signal_pending_unmatched(data)
            if entry:
                logger.info("place_bid rejected by server (unmatched): %s", data)
            else:
                logger.debug("Unhandled error WS message: %s", data)

        # The rejection tells us the true current price ("...current
        # highest bid (9200.00)") - use it to correct the cache immediately
        # instead of leaving it stale for every subsequent decision, and
        # re-dispatch right away so the worker retries with the corrected
        # price instead of sitting idle until some future WS event.
        if entry:
            payload = entry.get("payload") or {}
            auction_id = payload.get("pk")
            product_id = payload.get("product_id")
            rejected_price = _parse_rejected_price(data.get("message"))
            if rejected_price is not None:
                _update_cached_price(auction_id, product_id, rejected_price)
                item = _LAST_ITEM_MAP.get(auction_id, {}).get(product_id)
                if item:
                    process_external_snapshot({
                        auction_id: {
                            "AUCTION_ITEMS": {product_id: item},
                            "MIN_BID_AMOUNT": item.get("MIN_BID_AMOUNT", 1),
                        }
                    })
        return

    logger.debug("Unhandled WS message: %s", data)


def _on_error(ws, error):
    logger.error("WS error: %s", error)


def _on_close(ws, code, reason):
    logger.info("WS closed: %s %s", code, reason)
    global _global_ws
    with _global_ws_lock:
        _global_ws = None

    # Release any pending submit waiters so callers don't hang.
    with _PENDING_LOCK:
        to_release = list(_PENDING_SUBMITS.items())
        _PENDING_SUBMITS.clear()
    for _req_id, entry in to_release:
        entry["result"] = {"status": "error", "message": "ws-closed-before-echo"}
        entry["event"].set()


# ---------------------------------------------------------------------------
# start_ws_adapter — the composition root per bot process
# ---------------------------------------------------------------------------
def start_ws_adapter(username: str, password: str, auction_pk: str = None,
                     stop_event: "threading.Event" = None) -> None:
    """Log in, patch submit_bid, spin up the reconnect loop, idle.

    If `stop_event` is provided, the main idle loop exits when it's set,
    allowing the caller (bot_runner) to request a clean shutdown.
    """
    global AUCTION_PK
    if auction_pk:
        AUCTION_PK = auction_pk

    session = login_and_get_session(username, password)

    cookies_dict = session.cookies.get_dict()
    cookie_header = "; ".join(f"{k}={v}" for k, v in cookies_dict.items())

    origin = "http://localhost:8000"
    host = "localhost:8000"
    ws_url = WS_BASE + WS_JOIN_PATH + AUCTION_PK
    ws_headers = [
        f"Cookie: {cookie_header}",
        f"Host: {host}",
        "Connection: Upgrade",
        "Upgrade: websocket",
        "User-Agent: ws-adapter/1.0",
    ]

    logger.info("Connecting to WS: %s", ws_url)

    # ------------------------------------------------------------------
    # Submit path: persistent WS + request_id -> Event registry.
    # ------------------------------------------------------------------
    def _persistent_ws_submit(auction_id: str, item_id, bid_amount: float,
                              user_id: str = None, min_bid_amount: float = None):
        """Send a place_bid on the already-open WS and wait for the echo
        matching our request_id.

        Contract (matches the old ephemeral submitter):
            returns {"status": "success"|"error", "message": <dict or str>}
        """
        req_id = str(uuid.uuid4())
        payload = {
            "action": "place_bid",
            "product_id": item_id,
            "bid_amount": float(bid_amount),
            "pk": auction_id,
            "request_id": req_id,
        }

        # Register BEFORE sending so _on_message can never miss the echo.
        # `payload` is kept on the entry so a reply lacking auction/product
        # id (rejections and 'bid_added' confirmations both do) can still
        # be attributed to the right (auction, item) for cache correction.
        entry = {"event": threading.Event(), "result": None, "payload": payload}
        with _PENDING_LOCK:
            _PENDING_SUBMITS[req_id] = entry

        try:
            with _global_ws_lock:
                ws_ref = _global_ws
            if ws_ref is None:
                return {"status": "error", "message": "ws-not-connected"}

            try:
                with _global_ws_send_lock:
                    ws_ref.send(json.dumps(payload))
                logger.info("PERSISTENT WS SENT: %s", payload)
            except Exception as e:
                logger.exception("Persistent WS send failed")
                # Drop the dead socket; the reconnect loop will replace it.
                with _global_ws_lock:
                    try:
                        if _global_ws is ws_ref:
                            _global_ws.close()
                    except Exception:
                        pass
                return {"status": "error", "message": f"send-failed:{e}"}

            if not entry["event"].wait(timeout=PERSISTENT_SUBMIT_TIMEOUT):
                logger.warning(
                    "No echo within %.1fs for request_id=%s",
                    PERSISTENT_SUBMIT_TIMEOUT, req_id,
                )
                return {"status": "error", "message": "no-echo-timeout", "payload": payload}

            result = entry["result"] or {}
            if str(result.get("status", "")).lower() in ("error", "failed", "false"):
                return {
                    "status": "error",
                    "message": {"via": "persistent-ws", "response": result, "payload": payload},
                }
            return {
                "status": "success",
                "message": {"via": "persistent-ws", "response": result, "payload": payload},
            }

        finally:
            with _PENDING_LOCK:
                _PENDING_SUBMITS.pop(req_id, None)

    _patch_submit_bid(_persistent_ws_submit)

    # ------------------------------------------------------------------
    # Persistent receive WS with exponential-backoff reconnect.
    # ------------------------------------------------------------------
    def _new_ws_app():
        return websocket.WebSocketApp(
            ws_url,
            header=ws_headers,
            on_open=_on_open,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
        )

    def _run_loop():
        backoff = 1
        max_backoff = 30
        while True:
            try:
                ws_app = _new_ws_app()
                logger.info("Starting persistent WS (backoff=%s)", backoff)
                ws_app.run_forever(
                    ping_interval=PERSISTENT_PING_INTERVAL,
                    ping_timeout=PERSISTENT_PING_TIMEOUT,
                    origin=origin,
                )
                _close_global_ws()
                logger.warning("Persistent WS stopped; reconnecting after %s s", backoff)
            except Exception:
                logger.exception("Persistent WS run loop error; sleeping then retrying")
            time.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

    threading.Thread(target=_run_loop, daemon=True).start()

    try:
        if stop_event is not None:
            # Runner-managed mode: block until stop_event is set.
            while not stop_event.is_set():
                stop_event.wait(1)
            logger.info("stop_event set; shutting down ws adapter")
        else:
            # Standalone mode: block until Ctrl-C.
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping ws adapter")
    finally:
        _close_global_ws()


def _close_global_ws():
    """Clear the module-level WS reference and best-effort close it."""
    global _global_ws
    with _global_ws_lock:
        try:
            if _global_ws is not None:
                _global_ws.close()
        except Exception:
            pass
        _global_ws = None


def _patch_submit_bid(submitter):
    """Redirect AucApi.submit_bid (and AuctionPlayer.submit_bid) to the
    persistent WS submitter so the rest of the app is unaware of HTTP vs
    WebSocket."""
    AucApi.submit_bid = submitter
    logger.info("Patched AucApi.submit_bid -> persistent websocket submit")

    try:
        if hasattr(AuctionPlayer, "submit_bid"):
            AuctionPlayer.submit_bid = (
                lambda auction_id, item_id, bid_amount, user_id=None, min_bid_amount=None:
                submitter(auction_id, item_id, bid_amount, user_id, min_bid_amount)
            )
            logger.info("Patched AuctionPlayer.submit_bid -> persistent websocket submit")
        else:
            logger.info("AuctionPlayer module has no submit_bid attribute (skipping patch).")
    except Exception:
        logger.exception("Failed to patch AuctionPlayer.submit_bid")

    try:
        if hasattr(AuctionPlayer, "set_live_best_lookup"):
            AuctionPlayer.set_live_best_lookup(_lookup_cached_best)
            logger.info("Patched AuctionPlayer live-price lookup -> ws_adapter cache")
        else:
            logger.info("AuctionPlayer module has no set_live_best_lookup (skipping patch).")
    except Exception:
        logger.exception("Failed to patch AuctionPlayer live-price lookup")
