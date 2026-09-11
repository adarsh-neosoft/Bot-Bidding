"""Persona tuning: deterministic-seeded jitter for bid amount and delay.

Two delay paths are provided:
  * apply_delay                 — legacy fallback; uses persona.time_mean only.
  * apply_delay_with_duration   — primary path; driven by REMAINING auction time.

`_seeded_rng` makes every (bot_id, auction_id, step_index) triple reproducible
so replays and tests behave identically to live runs.
"""

import hashlib
import random
from typing import Dict


# ---------------------------------------------------------------------------
# Per-persona amount/delay tuning used by apply_amount and apply_delay.
# apply_delay_with_duration uses PERSONA_INTERVALS below instead.
# ---------------------------------------------------------------------------
PERSONA_PARAMS: Dict[str, Dict[str, float]] = {
    "patient":    {"time_mean": 18, "time_jitter": 12, "amount_jitter_pct": 5,  "aggression": 0.25},
    "aggressive": {"time_mean": 6,  "time_jitter": 4,  "amount_jitter_pct": 12, "aggression": 0.80},
    "bluffer":    {"time_mean": 7,  "time_jitter": 5,  "amount_jitter_pct": 20, "aggression": 0.50},
    "snipe":      {"time_mean": 3,  "time_jitter": 2,  "amount_jitter_pct": 6,  "aggression": 0.60},
}


# ---------------------------------------------------------------------------
# Duration-aware delay tuning used by apply_delay_with_duration.
#
# `early_sec` is anchored at REFERENCE_REMAINING_SEC (3 hours): the target
# delay when ~3h are left. Effective interval scales with
# sqrt(remaining / 3h), clamped to [EARLY_SCALE_MIN, EARLY_SCALE_MAX].
#
# `late_sec` is the target wait inside the last ENDGAME_WINDOW_SEC seconds.
#
# Anchor sanity check: at ~1h remaining delays should land in the 400-600s
# band for the slower personas. Numbers are chosen to hit that.
#
# Pacing is driven by REMAINING time only — behaviour at "20 min remaining"
# is the same regardless of how long the auction originally was.
# ---------------------------------------------------------------------------
REFERENCE_REMAINING_SEC = 3 * 3600     # 3 hours baseline.
EARLY_SCALE_MIN = 0.1                  # low floor so 2-5 min keeps decreasing.
EARLY_SCALE_MAX = 10.0                 # multi-day auctions don't explode.

# Endgame window: when remaining time drops below this many seconds, the
# interval blends from scaled_early DOWN toward late_sec. Widened from 8
# to 30 minutes so the endgame acceleration is already meaningful when a
# bot enters the last 8-10 minutes (previously the blend hadn't started
# at 8 min remaining, causing ~140s delays in the last phase).
ENDGAME_WINDOW_SEC = 30 * 60

# Per-persona tuning:
#   early_sec    target at REFERENCE_REMAINING_SEC (= 3h)
#   late_sec     target at t=0 inside the endgame window
#   jitter_frac  ±multiplicative noise
#
# `late_sec` values pulled down (patient 90→45, bluffer 30→20) so the
# endgame blend produces visibly shorter intervals by the last few minutes.
PERSONA_INTERVALS: Dict[str, Dict[str, float]] = {
    "patient":    {"early_sec": 800,  "late_sec": 45, "jitter_frac": 0.30},
    "aggressive": {"early_sec": 400,  "late_sec": 15, "jitter_frac": 0.25},
    "bluffer":    {"early_sec": 500,  "late_sec": 20, "jitter_frac": 0.40},
    "snipe":      {"early_sec": 1000, "late_sec": 6,  "jitter_frac": 0.20},
}


# ---------------------------------------------------------------------------
# Shared RNG: deterministic per (bot_id, auction_id, step_index).
# ---------------------------------------------------------------------------
def _seeded_rng(bot_id: str, auction_id: str, step_index: int = 0) -> random.Random:
    """Return a seeded Random instance.

    bot_id must be unique per bot. step_index MUST change between calls
    (e.g., from num_bids_placed) or the same random sequence is returned.
    """
    s = f"{bot_id}:{auction_id}:{step_index}"
    seed = int(hashlib.sha256(s.encode("utf-8")).hexdigest()[:16], 16) & 0xFFFFFFFF
    return random.Random(seed)


# ---------------------------------------------------------------------------
# Amount
# ---------------------------------------------------------------------------
def apply_amount(
    persona: str,
    base_amount: float,
    auction_features: Dict,
    bot_id: str,
    auction_id: str,
    step_index: int = 0,
) -> float:
    """Apply persona-driven jitter and an aggression bias to base_amount.

    Returns a rounded amount (not yet quantized to min_inc).
    """
    params = PERSONA_PARAMS.get(persona, PERSONA_PARAMS["patient"])
    rng = _seeded_rng(bot_id, auction_id, step_index)
    jitter_pct = params.get("amount_jitter_pct", 5)
    phase_ratio = float(auction_features.get("phase_ratio", 0.0))
    aggression = params.get("aggression", 0.0)

    bias = aggression * phase_ratio
    pct = rng.uniform(-jitter_pct, jitter_pct) / 100.0 + bias
    return max(1.0, round(base_amount * (1.0 + pct), 2))


# ---------------------------------------------------------------------------
# Delay (legacy, fallback path)
# ---------------------------------------------------------------------------
def apply_delay(
    persona: str,
    base_delay: int,
    auction_features: Dict,
    bot_id: str,
    auction_id: str,
    step_index: int = 0,
) -> int:
    """Legacy persona-driven delay. Kept for replay/compat; primary path is
    apply_delay_with_duration."""
    params = PERSONA_PARAMS.get(persona, PERSONA_PARAMS["patient"])
    rng = _seeded_rng(bot_id, auction_id, step_index + 1000)
    mean = params.get("time_mean", 10)
    jitter = params.get("time_jitter", 5)

    try:
        exp = int(rng.expovariate(1.0 / max(1.0, (base_delay + mean) / 2.0)))
    except Exception:
        exp = int(max(1, (base_delay + mean) // 2))
    gauss = int(round(rng.gauss(0, jitter / 2.0)))
    return max(1, exp + gauss)


# ---------------------------------------------------------------------------
# Delay (primary path: duration-aware, phase-aware)
# ---------------------------------------------------------------------------
def apply_delay_with_duration(
    persona: str,
    base_delay: int,           # unused; kept for signature compatibility
    time_remaining_seconds: int,
    total_duration_seconds: int,  # unused; kept for signature compatibility
    auction_features: Dict,
    bot_id: str,
    auction_id: str,
    step_index: int = 0,
) -> int:
    """Persona-driven delay in seconds, driven by REMAINING time.

    Flow:
      1. Scale early_sec by sqrt(remaining / 3h), clamped.
      2. If inside the endgame window, blend scaled_early -> late_sec with
         quadratic easing in remaining time.
      3. Apply seeded multiplicative jitter.
      4. Floor at 2s; ceil at remaining-5s so we never wait past expiry.
    """
    try:
        remaining = max(0.0, float(time_remaining_seconds or 0))
    except Exception:
        remaining = 0.0

    cfg = PERSONA_INTERVALS.get(persona, PERSONA_INTERVALS["patient"])
    early_sec = float(cfg["early_sec"])
    late_sec = float(cfg["late_sec"])
    jitter_frac = float(cfg.get("jitter_frac", 0.3))

    # 1. Scale the early baseline sub-linearly with remaining time.
    if remaining > 0:
        raw_scale = (remaining / REFERENCE_REMAINING_SEC) ** 0.5
    else:
        raw_scale = EARLY_SCALE_MIN
    early_scale = max(EARLY_SCALE_MIN, min(EARLY_SCALE_MAX, raw_scale))
    scaled_early = early_sec * early_scale

    # 2. Endgame acceleration kicks in only when remaining is small in
    #    absolute terms, not based on total duration.
    if remaining >= ENDGAME_WINDOW_SEC:
        base = scaled_early
    else:
        endgame_progress = 1.0 - (remaining / ENDGAME_WINDOW_SEC)
        endgame_progress = max(0.0, min(1.0, endgame_progress))
        eased = endgame_progress ** 2
        base = scaled_early + (late_sec - scaled_early) * eased

    # 3. Seeded jitter. step_index offset by 1000 keeps this RNG stream
    #    independent from apply_amount's.
    rng = _seeded_rng(bot_id, auction_id, step_index + 1000)
    noise_mult = 1.0 + rng.uniform(-jitter_frac, jitter_frac)
    delay = int(round(base * noise_mult))

    # 4. Clamp floor and ceiling.
    delay = max(2, delay)
    if remaining > 5:
        delay = min(delay, int(remaining) - 5)
    else:
        delay = max(2, min(delay, 3))

    return delay
