"""
ed_poll.py
- Poll Ed Discussion for new posts and post a customizable message to a Slack channel.
- Can be run locally on computer or setup on Github Actions or cron job or VM (EC2, etc).
- edit default config, message, and cli arguments to modify message

(environment variables, needs manual input)
- ED_API_TOKEN        : your Ed API token
- SLACK_WEBHOOK_URL   : Slack Incoming Webhook URL
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests


# =============================================================================
# 1) CONFIG DEFAULTS
# =============================================================================

DEFAULT_ED_API_HOST = "https://us.edstem.org/api"
DEFAULT_REGION_PREFIX = "us"  # used to build browser URLs: https://edstem.org/us/...
DEFAULT_POLL_SECONDS = 300    # 5 minutes
DEFAULT_FETCH_LIMIT = 30      # how many newest discussions to fetch each poll
DEFAULT_MAX_POSTS_PER_RUN = 50  # cap to avoid spamming if state resets


# =============================================================================
# 2) MESSAGE CUSTOMIZATION
# =============================================================================

def build_discussion_url(course_id: int, discussion_id: int, region_prefix: str = DEFAULT_REGION_PREFIX) -> str:
    """
    Build a web URL (what humans click), not the API URL.
    If your Ed instance uses a different region path, change region_prefix.
    """
    return f"https://edstem.org/{region_prefix}/courses/{course_id}/discussion/{discussion_id}"


def format_slack_message(discussion: Dict[str, Any], course_id: int, region_prefix: str) -> str:
    """
    ⭐ Customize your Slack message format HERE. ⭐

    Available data depends on Ed response. Common fields used below:
      - discussion["id"] (int)
      - discussion["title"] or discussion["subject"]
      - discussion["created_at"], discussion["updated_at"]
      - discussion["user"]["name"] sometimes exists (author)

    Return: a plain-text Slack message.
    """
    did = discussion.get("id")
    title = discussion.get("title") or discussion.get("subject") or "(untitled)"
    created_at = discussion.get("created_at") or ""
    author = (
        (discussion.get("user") or {}).get("name")
        or (discussion.get("author") or {}).get("name")
        or ""
    )

    url = build_discussion_url(course_id, int(did), region_prefix) if isinstance(did, int) else ""
    
    if author:
        return f"📝 *{title}* (by {author})\n{url}"
    return f"📝 *{title}*\n{url}"


# =============================================================================
# 3) LOW-LEVEL HELPERS (HTTP, state, etc.)
# =============================================================================

def iso_now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def request_json(method: str, url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Make an HTTP request and parse JSON, raising a helpful error on failure.
    """
    status = None
    body_preview = ""
    try:
        r = requests.request(method, url, headers=headers, params=params, timeout=30)
        status = r.status_code
        body_preview = (r.text or "")[:500]

        if not (200 <= r.status_code <= 299):
            raise RuntimeError(f"HTTP {r.status_code} from {url}: {body_preview}")

        return r.json()

    except Exception as e:
        raise RuntimeError(f"Request failed ({method} {url}). Status={status}. Body={body_preview}. Err={e}") from e


def load_state(path: str) -> Dict[str, Any]:
    """
    State tracks the last discussion id we've posted.
    """
    if not os.path.exists(path):
        return {"last_seen_id": 0, "last_run_utc": None}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "last_seen_id" not in data:
            data["last_seen_id"] = 0
        return data
    except Exception:
        # Fail safe: start from 0, but do not spam thanks to max-per-run cap
        return {"last_seen_id": 0, "last_run_utc": None, "warning": "state file unreadable; reset state"}


def save_state(path: str, state: Dict[str, Any]) -> None:
    """
    Atomic-ish write: write temp file then replace.
    """
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, path)


def fetch_latest_threads(ed_api_host: str, token: str, course_id: int, limit: int) -> List[Dict[str, Any]]:
    """
    Fetch latest threads from Ed for a course.
    """
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{ed_api_host}/courses/{course_id}/threads"

    data = request_json("GET", url, headers=headers, params={"limit": limit, "sort": "new"})

    threads = data.get("threads")

    # Some instances may nest differently; fallback safely
    if not isinstance(threads, list):
        threads = (data.get("data") or {}).get("threads") or []
    # Keep only dicts
    return [d for d in threads if isinstance(d, dict)]


def post_to_slack(webhook_url: str, text: str) -> None:
    """
    Send a text message to Slack Incoming Webhook.
    """
    resp = requests.post(webhook_url, json={"text": text}, timeout=20)
    if not (200 <= resp.status_code <= 299):
        raise RuntimeError(f"Slack webhook failed: HTTP {resp.status_code}: {(resp.text or '')[:300]}")


# =============================================================================
# 4) MAIN POLLING LOGIC
# =============================================================================

def poll_once(args: argparse.Namespace) -> int:
    """
    Poll Ed once. If new discussions exist, post them (or print them in dry-run).
    Returns number of messages posted/printed.
    """
    token = os.getenv("ED_API_TOKEN")
    if not token:
        raise RuntimeError("Missing ED_API_TOKEN environment variable.")

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    state = load_state(args.state_file)
    last_seen_id = int(state.get("last_seen_id") or 0)

    threads = fetch_latest_threads(args.ed_api_host, token, args.course_id, args.limit)

    # Filter to numeric IDs and only those newer than last_seen_id
    candidates = [d for d in threads if isinstance(d.get("id"), int)]
    new_posts = [d for d in candidates if d["id"] > last_seen_id]

    # Oldest-first so Slack shows them in chronological order
    new_posts.sort(key=lambda d: d["id"])

    # Safety cap to prevent accidental spam if state resets
    new_posts = new_posts[: args.max_posts_per_run]

    if not new_posts:
        return 0

    posted = 0
    max_id = last_seen_id

    for d in new_posts:
        msg = format_slack_message(d, args.course_id, args.region_prefix)

        if args.dry_run or not webhook_url:
            # dry-run OR webhook missing: just print to stdout
            print(msg)
            posted += 1
        else:
            post_to_slack(webhook_url, msg)
            posted += 1

        max_id = max(max_id, int(d["id"]))

    if not args.dry_run:
        state["last_seen_id"] = max_id
        state["last_run_utc"] = iso_now_utc()
        save_state(args.state_file, state)

    return posted


def main() -> None:
    # -------------------------------------------------------------------------
    # CLI arguments
    # -------------------------------------------------------------------------
    p = argparse.ArgumentParser(description="Poll Ed for new discussions and post to Slack via webhook.")
    p.add_argument("--course-id", type=int, required=True, help="Ed course ID to poll")
    p.add_argument("--ed-api-host", type=str, default=DEFAULT_ED_API_HOST, help="Ed API base URL")
    p.add_argument("--region-prefix", type=str, default=DEFAULT_REGION_PREFIX, help="Ed web URL region prefix (us/au/...)")
    p.add_argument("--interval", type=int, default=DEFAULT_POLL_SECONDS, help="Polling interval in seconds")
    p.add_argument("--limit", type=int, default=DEFAULT_FETCH_LIMIT, help="How many newest discussions to fetch each poll")
    p.add_argument("--max-posts-per-run", type=int, default=DEFAULT_MAX_POSTS_PER_RUN, help="Max messages per poll run")
    p.add_argument("--state-file", type=str, default=".ed_state.json", help="Path to state file (dedupe)")
    p.add_argument("--once", action="store_true", help="Run one poll and exit")
    p.add_argument("--dry-run", action="store_true", help="Do not POST to Slack; print messages instead")
    args = p.parse_args()

    # -------------------------------------------------------------------------
    # Run once or loop forever
    # -------------------------------------------------------------------------
    if args.once:
        try:
            poll_once(args)
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
        return

    while True:
        try:
            poll_once(args)
            sys.stdout.flush()
        except Exception as e:
            print(f"[{iso_now_utc()}] ERROR: {e}", file=sys.stderr)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()