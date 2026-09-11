"""Lightweight fuzzy-style helper that maps auction features to a bid
category (low/medium/high) and a numeric score.

Inputs (all in [0, 1]):
  price_to_cover : how much of the gap to threshold is covered
  active_users   : normalized recent bid rate
  auction_phase  : 0 at start, 1 at end

Outputs:
  category : 'low' | 'medium' | 'high'
  score    : numeric [0, 1], higher => more aggressive
"""

from typing import Dict, Tuple


# ---------------------------------------------------------------------------
# Membership functions (triangular)
# ---------------------------------------------------------------------------
def _membership_low(x: float) -> float:
    if x <= 0.2:
        return 1.0
    if x >= 0.5:
        return 0.0
    return (0.5 - x) / (0.5 - 0.2)


def _membership_medium(x: float) -> float:
    if 0.2 < x < 0.5:
        return (x - 0.2) / (0.5 - 0.2)
    if 0.5 <= x <= 0.8:
        return (0.8 - x) / (0.8 - 0.5)
    return 0.0


def _membership_high(x: float) -> float:
    if x <= 0.5:
        return 0.0
    if x >= 0.8:
        return 1.0
    return (x - 0.5) / (0.8 - 0.5)


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def compute_bid_category(inputs: Dict[str, float]) -> Tuple[str, float]:
    """Return (category, score). See module docstring for input contract."""
    p = _clamp01(inputs.get("price_to_cover", 0.0))
    u = _clamp01(inputs.get("active_users", 0.0))
    ph = _clamp01(inputs.get("auction_phase", 0.0))

    # Memberships for price_to_cover and active_users
    p_low, p_med, p_high = _membership_low(p), _membership_medium(p), _membership_high(p)
    u_low, u_med, u_high = _membership_low(u), _membership_medium(u), _membership_high(u)

    # Phase has simpler piecewise-linear memberships
    ph_start = 1.0 - ph
    ph_mid = 1.0 - abs(ph - 0.5) * 2.0
    ph_end = ph

    # Rule activations (max across OR'd rules)
    high_act = max(
        min(p_high, u_high, ph_end),
        min(p_high, u_med, ph_mid),
        min(p_med, u_high, ph_end),
    )
    medium_act = max(
        min(p_med, u_med, ph_mid),
        min(p_high, u_low, ph_mid),
        min(p_low, u_high, ph_mid),
    )
    low_act = max(
        min(p_low, u_low, ph_start),
        min(p_low, u_med, ph_mid),
        min(p_med, u_low, ph_start),
    )

    total = high_act + medium_act + low_act
    if total <= 0:
        return "low", 0.0

    # Weighted defuzzification: each category has a representative score
    # (low=0.15, medium=0.5, high=0.85) and we blend by activation weight.
    score = (low_act * 0.15 + medium_act * 0.5 + high_act * 0.85) / total

    # Category = strongest activation, with lex tiebreaker for determinism.
    cat_map = [("high", high_act), ("medium", medium_act), ("low", low_act)]
    cat_map.sort(key=lambda t: (-t[1], t[0]))
    return cat_map[0][0], float(round(score, 4))
