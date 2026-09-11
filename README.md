# BotService — Deterministic Persona Bot (Python 3.11)

This scaffold implements a BotService for auctions:
- Deterministic persona selection, seeded per (bot_id, auction_id)
- Persona-specific amount & timing logic
- Lightweight fuzzy logic helper to produce bid categories
- FastAPI endpoints for decision and admin replay
- In-memory store (replace with Redis/DB in prod)
- Simulator for tuning

Run:
1. python -m venv .venv
2. source .venv/bin/activate    # Windows: .venv\Scripts\activate
3. pip install -r requirements.txt
4. uvicorn app.main:app --reload --port 8080
