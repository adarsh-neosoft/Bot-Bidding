"""Base amount and base delay heuristics consumed by DecisionService.

These are phase- and rate-aware but not persona-aware. The persona layer
applies jitter and bias on top of whatever is returned here.
"""

from typing import Dict


def compute_base_amount(auction_state: Dict) -> float:
    """Base bid amount heuristic.

    Takes a small fraction of the remaining distance between best and
    threshold, bounded below by min_inc and above by 10 * min_inc. Returns
    exactly min_inc if the gap is already within 5 * min_inc.

    Expected keys: best_price, threshold_price, min_inc_price.
    """
    best_price = float(auction_state.get("best_price", 0.0))
    threshold = float(auction_state.get("threshold_price", best_price + 1000))
    min_inc = float(auction_state.get("min_inc_price", 1.0))
    dist = max(0.0, threshold - best_price)

    if dist <= 5 * min_inc:
        return round(min_inc, 2)

    base = min(max(min_inc, dist * 0.05), 10 * min_inc)
    return round(base, 2)


def estimate_base_delay(auction_state: Dict) -> int:
    """Base delay heuristic in seconds.

    Inverse to recent_bid_rate (more active auctions -> shorter base delay)
    and scaled by time_left_pct (more time left -> longer base delay).

    Expected keys: recent_bid_rate, time_left_pct (both in [0, 1]).
    """
    recent_rate = float(auction_state.get("recent_bid_rate", 1.0))
    time_left_pct = float(auction_state.get("time_left_pct", 0.5))
    return max(1, int((1.0 / max(0.1, recent_rate)) * (1.0 + time_left_pct * 2.0) * 10))
