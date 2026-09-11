"""One-shot maintenance: wipe stray `bots_disabled` flags from Redis.

Usage:
    python scripts/clear_bots_disabled.py                 # dry-run (default)
    python scripts/clear_bots_disabled.py --apply         # actually delete
    python scripts/clear_bots_disabled.py --auction AUC_1 # target one auction

Exits non-zero if Redis isn't reachable.

Why this exists:
    `bots_disabled` is a kill switch stored on `auction:<id>` hashes. Nothing
    in the current codebase sets it, but it may have been left over from an
    earlier version, manual `redis-cli hset`, or an aborted experiment. A
    set flag permanently blocks bots from bidding on that auction. This
    script clears them safely.
"""

import argparse
import sys

# Allow "python scripts/clear_bots_disabled.py" to find the project root.
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from state_store import get_redis  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Actually delete. Without this flag, dry-run only.")
    parser.add_argument("--auction", default=None,
                        help="Limit to a single auction id. Omit to scan all.")
    args = parser.parse_args()

    r = get_redis()
    if r is None:
        print("ERROR: Redis unreachable. Check REDIS_URL in your .env.",
              file=sys.stderr)
        return 2

    # Decide which keys to examine.
    if args.auction:
        keys = [f"auction:{args.auction}"]
    else:
        keys = list(r.scan_iter(match="auction:*"))

    print(f"Scanning {len(keys)} auction hash(es)...")

    found = 0
    cleared = 0
    for key in keys:
        # Skip sub-hashes like 'auction:X:bot:Y'; we only want the top-level.
        # Those have exactly one ':' separator.
        if key.count(":") != 1:
            continue

        val = r.hget(key, "bots_disabled")
        if val is None:
            continue

        found += 1
        print(f"  {key}  bots_disabled={val!r}")
        if args.apply:
            r.hdel(key, "bots_disabled")
            cleared += 1

    print()
    if not args.apply:
        print(f"DRY RUN: would clear {found} flag(s). Re-run with --apply to delete.")
    else:
        print(f"Cleared {cleared} flag(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
