"""Decision orchestrator.

Responsibilities:
  * Pick (or reuse) a persona for the bot on this auction.
  * Compute a fuzzy bid category from auction state.
  * Compute a base amount (heuristic) and a duration-aware delay (persona).
  * Return a decision response containing the delay, a placeholder
    bid_amount (min_inc by policy), and metadata for replay.

The caller (AuctionPlayer) decides the actual bid price, using best_now +
min_inc instead of decision["bid_amount"]. The amount pipeline is kept
alive here for metadata / replay / future use.
"""

from typing import Any, Dict

from app.chooser import select_persona
from app.fuzzy_logic import compute_bid_category
from app.hybrid_core import compute_base_amount, estimate_base_delay
from app.persona import apply_amount, apply_delay_with_duration
from app.utils import decision_inputs_hash, now_iso


class DecisionService:
    """Single place to orchestrate a decision.

    decide() returns the response payload plus an `updated_state` dict the
    caller is expected to persist.
    """

    # Bot only participates once this many seconds remain in the auction.
    BID_WINDOW_SECONDS = 15 * 60

    def __init__(self, agent_name: str = "AutoPlayer-v2"):
        self.agent = agent_name

    # ---- public API ------------------------------------------------------

    def decide(self, req_obj, state: Dict[str, Any]) -> Dict[str, Any]:
        """Orchestrate one decision for req_obj given the current state."""
        remaining = float(getattr(req_obj, "remaining_duration", 0) or 0)
        if remaining > self.BID_WINDOW_SECONDS:
            return {
                "status": "skip",
                "reason": "outside_bidding_window",
                "bid_amount": None,
                "delay_seconds": 0,
                "metadata": {
                    "is_automated": True,
                    "agent": self.agent,
                    "remaining_duration": remaining,
                    "bid_window_seconds": self.BID_WINDOW_SECONDS,
                    "generated_at": now_iso(),
                },
                "updated_state": state,
            }

        persona = self._ensure_persona(req_obj, state)
        fuzzy_inputs = self._build_fuzzy_inputs(req_obj)
        category, fscore = compute_bid_category(fuzzy_inputs)

        base_amount = self._base_amount(req_obj)
        base_delay = self._base_delay(req_obj)

        amount = apply_amount(
            persona,
            base_amount * (1.0 + (fscore - 0.5) * 0.4),
            req_obj.antecedent_features,
            req_obj.bot_user_id,
            req_obj.auction_id,
            req_obj.step_index,
        )
        delay = apply_delay_with_duration(
            persona,
            base_delay,
            req_obj.remaining_duration,
            req_obj.total_duration,
            req_obj.antecedent_features,
            req_obj.bot_user_id,
            req_obj.auction_id,
            req_obj.step_index,
        )

        # Amount policy (see AuctionPlayer for rationale): callers use
        # best_now + min_inc. We still report min_inc here for metadata.
        quantized = float(req_obj.min_inc_price)

        self._update_state_bookkeeping(state, quantized)

        return {
            "status": "ok",
            "persona": persona,
            "bid_amount": quantized,
            "delay_seconds": delay,
            "metadata": self._build_metadata(req_obj, persona, category, fscore),
            "updated_state": state,
        }

    # ---- helpers ---------------------------------------------------------

    @staticmethod
    def _ensure_persona(req_obj, state: Dict[str, Any]) -> str:
        """Re-select the persona on every decision.

        Previously we froze the first persona choice on `state.persona_current`
        for the life of the auction. That was wrong: a bot picked as
        'patient' at auction start would remain 'patient' even in the last
        minute when 'snipe' or 'aggressive' is the right profile. Now we
        re-compute each call and simply record the current choice on state
        so downstream consumers (logs, replay) can see what we picked.
        """
        persona = select_persona(
            req_obj.antecedent_features, req_obj.bot_user_id, req_obj.auction_id
        )
        state["persona_current"] = persona
        return persona

    @staticmethod
    def _build_fuzzy_inputs(req_obj) -> Dict[str, float]:
        threshold = req_obj.threshold_price
        best_price = req_obj.auction_state.get("best_price", 0)
        return {
            "price_to_cover": (threshold - best_price) / max(1, threshold),
            "active_users": req_obj.antecedent_features.get("recent_bid_rate", 0.0),
            "auction_phase": req_obj.antecedent_features.get("phase_ratio", 0.0),
        }

    @staticmethod
    def _base_amount(req_obj) -> float:
        return compute_base_amount({
            "best_price": req_obj.auction_state.get("best_price", 0),
            "threshold_price": req_obj.threshold_price,
            "min_inc_price": req_obj.min_inc_price,
            "recent_bid_rate": req_obj.antecedent_features.get("recent_bid_rate", 1.0),
            "phase_ratio": req_obj.antecedent_features.get("phase_ratio", 0.0),
        })

    @staticmethod
    def _base_delay(req_obj) -> int:
        return estimate_base_delay({
            "recent_bid_rate": req_obj.antecedent_features.get("recent_bid_rate", 1.0),
            "time_left_pct": req_obj.antecedent_features.get("time_left_pct", 0.5),
        })

    @staticmethod
    def _update_state_bookkeeping(state: Dict[str, Any], quantized: float) -> None:
        state["num_bids_placed"] = state.get("num_bids_placed", 0) + 1
        state["last_action_time"] = now_iso()
        state["budget_remaining"] = state.get("budget_remaining", 10000) - quantized

    def _build_metadata(self, req_obj, persona: str, category: str, fscore: float) -> Dict[str, Any]:
        return {
            "is_automated": True,
            "agent": self.agent,
            "persona": persona,
            "fuzzy_category": category,
            "fuzzy_score": fscore,
            "decision_inputs_hash": decision_inputs_hash({
                "auction_id": req_obj.auction_id,
                "antecedent_features": req_obj.antecedent_features,
                "auction_state": req_obj.auction_state,
                "min_inc_price": req_obj.min_inc_price,
                "max_inc_price": req_obj.max_inc_price,
                "threshold_price": req_obj.threshold_price,
            }),
            "generated_at": now_iso(),
        }
