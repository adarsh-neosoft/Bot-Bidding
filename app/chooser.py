"""Persona selection via weighted feature scoring."""

from typing import Dict

from config import PERSONAS, WEIGHT_MATRIX


def normalize_features(raw: Dict[str, float]) -> Dict[str, float]:
    """Clamp each feature value to [0, 1]. Non-numeric values coerce to 0."""
    out: Dict[str, float] = {}
    for name, value in raw.items():
        try:
            fv = float(value)
        except Exception:
            fv = 0.0
        out[name] = max(0.0, min(1.0, fv))
    return out


def score_persona(features: Dict[str, float], persona: str) -> float:
    """Dot product of feature values and this persona's weight row."""
    weights = WEIGHT_MATRIX.get(persona, {})
    return sum(weights.get(name, 0.0) * value for name, value in features.items())


def select_persona(features_raw: Dict[str, float], bot_id: str, auction_id: str) -> str:
    """Pick the highest-scoring persona. Ties break lexicographically on
    the persona name so results are deterministic."""
    features = normalize_features(features_raw)
    scores = {p: score_persona(features, p) for p in PERSONAS}
    sorted_items = sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))
    return sorted_items[0][0]
