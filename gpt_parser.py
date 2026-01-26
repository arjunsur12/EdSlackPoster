#!/usr/bin/env python3
"""
Export Ed Discussion threads into a CSV of (question, answer) pairs suitable for GPT Knowledge/RAG.

Usage:
  export ED_API_TOKEN=...
  python ed_export_qa.py --course-id 12345 --out ed_qa.csv --limit 200

Optional:
  --since-id 100000   (only export threads with id > since-id)
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from pathlib import Path

import requests

env_path = Path(__file__).resolve().parent / ".env"
print("Loading .env from:", env_path, "exists?", env_path.exists())
load_dotenv(dotenv_path=env_path, override=True)

DEFAULT_ED_API_HOST = "https://us.edstem.org/api"
DEFAULT_REGION_PREFIX = "us"


def build_discussion_url(course_id: int, discussion_id: int, region_prefix: str = DEFAULT_REGION_PREFIX) -> str:
    return f"https://edstem.org/{region_prefix}/courses/{course_id}/discussion/{discussion_id}"


def request_json(method: str, url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    r = requests.request(method, url, headers=headers, params=params, timeout=30)
    if not (200 <= r.status_code <= 299):
        preview = (r.text or "")[:500]
        raise RuntimeError(f"HTTP {r.status_code} from {url}: {preview}")
    return r.json()


def fetch_latest_threads(ed_api_host: str, token: str, course_id: int, limit: int) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{ed_api_host}/courses/{course_id}/threads"
    data = request_json("GET", url, headers=headers, params={"limit": limit, "sort": "new"})

    threads = data.get("threads")
    if not isinstance(threads, list):
        threads = (data.get("data") or {}).get("threads") or []
    return [t for t in threads if isinstance(t, dict)]


def fetch_thread_detail(ed_api_host: str, token: str, course_id: int, thread_id: int) -> Dict[str, Any]:
    """
    Many Ed deployments provide a thread-detail endpoint. If your instance differs,
    print the JSON once and adjust this function.
    """
    headers = {"Authorization": f"Bearer {token}"}

    # Try multiple endpoint patterns + also try the alternate host if needed.
    hosts = [ed_api_host]
    if ed_api_host == "https://us.edstem.org/api":
        hosts.append("https://edstem.org/api")  # some deployments route detail calls here

    candidate_urls = []
    for host in hosts:
        candidate_urls.extend([
            # Common patterns
            #f"{host}/courses/{course_id}/threads/{thread_id}",
            #f"{host}/courses/{course_id}/threads/{thread_id}/view",
            #f"{host}/courses/{course_id}/threads/{thread_id}/posts",

            # Website uses /discussion/<id>; some APIs mirror that
            #f"{host}/courses/{course_id}/discussion/{thread_id}",
            #f"{host}/courses/{course_id}/discussions/{thread_id}",

            # Non course-scoped fallbacks
            f"{host}/threads/{thread_id}",
            #f"{host}/discussion/{thread_id}",
            #f"{host}/discussions/{thread_id}",
        ])

    last_err = None
    for url in candidate_urls:
        try:
            # keep your debug prints if you want:
            print("Trying:", url)
            return request_json("GET", url, headers=headers)
        except Exception as e:
            last_err = e

    raise RuntimeError(f"Could not fetch thread detail for {thread_id}. Last error: {last_err}")


_TAG_RE = re.compile(r"<[^>]+>")


def html_to_text(s: str) -> str:
    """
    Ed bodies often come as HTML. This quick scrub is usually "good enough" for CSV.
    If you want prettier conversion, swap this for BeautifulSoup / markdownify.
    """
    if not s:
        return ""
    s = s.replace("\r", "\n")
    s = _TAG_RE.sub("", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def scrub_pii(text: str) -> str:
    """
    Lightweight PII scrub: emails + common name patterns (optional).
    You can expand this depending on your privacy requirements.
    """
    if not text:
        return ""
    text = re.sub(r"[\w\.-]+@[\w\.-]+\.\w+", "[REDACTED_EMAIL]", text)
    return text


def pick_fields_thread_container(detail_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Some responses nest thread data under 'thread' or 'data'.
    Return a dict that "looks like" the thread object.
    """
    if isinstance(detail_json.get("thread"), dict):
        return detail_json["thread"]
    if isinstance((detail_json.get("data") or {}).get("thread"), dict):
        return detail_json["data"]["thread"]
    if isinstance(detail_json.get("data"), dict):
        return detail_json["data"]
    return detail_json


def extract_question_and_answers(thread_obj: Dict[str, Any]) -> tuple[str, List[str]]:
    """
    Heuristics:
      - Question text: body/content/post text fields
      - Answers: look for an 'answers' list, 'comments' list, or 'posts' list
    """
    # Question body candidates
    q_raw = (
        thread_obj.get("content")
        or thread_obj.get("body")
        or thread_obj.get("text")
        or (thread_obj.get("post") or {}).get("content")
        or ""
    )
    question = scrub_pii(html_to_text(q_raw))

    answers: List[str] = []

    # Common containers
    for key in ["answers", "responses", "comments", "posts"]:
        val = thread_obj.get(key)
        if isinstance(val, list):
            for item in val:
                if not isinstance(item, dict):
                    continue
                a_raw = item.get("content") or item.get("body") or item.get("text") or ""
                a_txt = scrub_pii(html_to_text(a_raw))
                if a_txt:
                    answers.append(a_txt)

    # Sometimes answers are nested under something like thread_obj["comments"]["data"]
    comments = thread_obj.get("comments")
    if isinstance(comments, dict):
        data = comments.get("data") or comments.get("comments")
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    a_raw = item.get("content") or item.get("body") or item.get("text") or ""
                    a_txt = scrub_pii(html_to_text(a_raw))
                    if a_txt:
                        answers.append(a_txt)

    # De-dupe while preserving order
    seen = set()
    uniq = []
    for a in answers:
        if a not in seen:
            uniq.append(a)
            seen.add(a)

    return question, uniq


def choose_best_answer(answers: List[str]) -> str:
    """
    If you can identify endorsed/accepted/staff answers from metadata, do that here.
    For now: pick the longest non-empty answer (often the most complete TA reply).
    """
    if not answers:
        return ""
    return max(answers, key=len)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--course-id", type=int, required=True)
    ap.add_argument("--out", type=str, default="ed_qa.csv")
    ap.add_argument("--ed-api-host", type=str, default=DEFAULT_ED_API_HOST)
    ap.add_argument("--region-prefix", type=str, default=DEFAULT_REGION_PREFIX)
    ap.add_argument("--limit", type=int, default=200, help="How many newest threads to list")
    ap.add_argument("--since-id", type=int, default=0, help="Only export threads with id > since-id")
    ap.add_argument("--max-threads", type=int, default=500, help="Safety cap")
    args = ap.parse_args()

    token = os.getenv("ED_API_TOKEN")
    if not token:
        raise RuntimeError("Missing ED_API_TOKEN environment variable.")

    threads = fetch_latest_threads(args.ed_api_host, token, args.course_id, args.limit)
    threads = [t for t in threads if isinstance(t.get("id"), int) and t["id"] > args.since_id]
    threads.sort(key=lambda t: t["id"])
    threads = threads[: args.max_threads]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "thread_id",
                "url",
                "title",
                "question",
                "answer",
                "all_answers",
                "tags",
                "created_at",
                "updated_at",
            ],
        )
        w.writeheader()

        idx = 0
        for t in threads:
            tid = int(t["id"])
            title = t.get("title") or t.get("subject") or "(untitled)"
            created_at = t.get("created_at") or ""
            updated_at = t.get("updated_at") or ""
            tags = t.get("tags") or t.get("category") or ""

            try:
                detail = fetch_thread_detail(args.ed_api_host, token, args.course_id, tid)
            except RuntimeError as e:
                print(f"[WARN] Skipping thread {tid}: {e}")
                continue

            thread_obj = pick_fields_thread_container(detail)

            question, answers = extract_question_and_answers(thread_obj)
            best = choose_best_answer(answers)

            w.writerow(
                {
                    "title": scrub_pii(str(title)),
                    "question": question,
                    "answer": best,
                    "all_answers": "\n\n---\n\n".join(answers),
                    "tags": str(tags),
                }
            )
            idx += 1

            if idx == 100:
                break

    # eventually change to len(threads)
    print(f"Wrote {100} threads to {args.out}")
    
if __name__ == "__main__":
    main()
