from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
import json
import os
import urllib.parse
import urllib.request

from bait_engine.intake.contracts import HuntTarget


logger = logging.getLogger(__name__)


REDDIT_PUBLIC_BASE = "https://www.reddit.com"
REDDIT_OAUTH_BASE = "https://oauth.reddit.com"
X_RECENT_SEARCH_URL = "https://api.x.com/2/tweets/search/recent"


def supported_hunt_sources() -> tuple[str, ...]:
    return ("jsonl_file", "reddit_listing", "reddit_search", "x_search_recent")


def source_requirements(source: str) -> dict[str, Any]:
    if source == "jsonl_file":
        return {
            "required": ["file_path"],
            "description": "Reads local JSONL records with body/thread metadata and ranks them for promotion.",
        }
    if source == "reddit_listing":
        return {
            "required": ["subreddit"],
            "description": "Fetches Reddit submissions from a subreddit listing (`hot`, `new`, `top`, `rising`). OAuth token optional.",
        }
    if source == "reddit_search":
        return {
            "required": ["query"],
            "description": "Searches Reddit posts by query; can be restricted to a subreddit. OAuth token optional.",
        }
    if source == "x_search_recent":
        return {
            "required": ["query", "bearer_token|X_BEARER_TOKEN|TWITTER_BEARER_TOKEN"],
            "description": "Fetches recent tweets from the X recent-search API.",
        }
    raise ValueError(f"unknown hunt source: {source}")


def _json_get(url: str, *, headers: dict[str, str] | None = None, timeout_seconds: float = 15.0) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=headers or {}, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
        raw = response.read().decode("utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object from {url}")
    return data


def _clean_author(value: Any) -> str | None:
    author = str(value or "").strip()
    if not author or author == "[deleted]":
        return None
    return author.lstrip("@/")


def _reddit_headers(access_token: str | None, user_agent: str | None) -> dict[str, str]:
    headers = {"User-Agent": user_agent or "bait-engine-v2/0.1"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    return headers


def _reddit_target_from_post(item: dict[str, Any], source_driver: str) -> HuntTarget:
    raw_id = str(item.get("name") or f"t3_{item.get('id')}")
    title = str(item.get("title") or "").strip() or None
    selftext = str(item.get("selftext") or "").strip()
    body = selftext or str(item.get("title") or "").strip()
    permalink = item.get("permalink")
    url = f"https://reddit.com{permalink}" if permalink else item.get("url")
    author = _clean_author(item.get("author"))
    metadata = {
        "score": item.get("score"),
        "num_comments": item.get("num_comments"),
        "subreddit": item.get("subreddit"),
        "permalink": permalink,
        "url": url,
        "created_utc": item.get("created_utc"),
        "upvote_ratio": item.get("upvote_ratio"),
    }
    context = {
        "platform": "reddit",
        "thread_id": raw_id,
        "subject": title,
        "root_author_handle": author,
        "messages": [
            {
                "message_id": raw_id,
                "author_handle": author,
                "body": body,
                "metadata": {"score": item.get("score"), "num_comments": item.get("num_comments")},
            }
        ],
        "metadata": metadata,
    }
    return HuntTarget(
        source_driver=source_driver,
        source_item_id=raw_id,
        platform="reddit",
        thread_id=raw_id,
        reply_to_id=raw_id,
        author_handle=author,
        subject=title,
        body=body,
        permalink=url,
        context=context,
        metadata=metadata,
    )


def _fetch_reddit_listing(
    *,
    subreddit: str,
    sort: str = "new",
    limit: int = 25,
    access_token: str | None = None,
    user_agent: str | None = None,
    timeout_seconds: float = 15.0,
) -> list[HuntTarget]:
    normalized_sort = sort if sort in {"hot", "new", "top", "rising"} else "new"
    base = REDDIT_OAUTH_BASE if access_token else REDDIT_PUBLIC_BASE
    query = urllib.parse.urlencode({"limit": max(1, min(int(limit), 100)), "raw_json": 1})
    url = f"{base}/r/{urllib.parse.quote(subreddit)}/{normalized_sort}.json?{query}"
    payload = _json_get(url, headers=_reddit_headers(access_token, user_agent), timeout_seconds=timeout_seconds)
    children = (((payload.get("data") or {}).get("children") or []))
    return [_reddit_target_from_post((item.get("data") or {}), "reddit_listing") for item in children if isinstance(item, dict)]


def _fetch_reddit_search(
    *,
    query: str,
    subreddit: str | None = None,
    sort: str = "new",
    limit: int = 25,
    access_token: str | None = None,
    user_agent: str | None = None,
    timeout_seconds: float = 15.0,
) -> list[HuntTarget]:
    base = REDDIT_OAUTH_BASE if access_token else REDDIT_PUBLIC_BASE
    params = {
        "q": query,
        "sort": sort,
        "limit": max(1, min(int(limit), 100)),
        "type": "link",
        "raw_json": 1,
    }
    if subreddit:
        params["restrict_sr"] = 1
        url = f"{base}/r/{urllib.parse.quote(subreddit)}/search.json?{urllib.parse.urlencode(params)}"
    else:
        url = f"{base}/search.json?{urllib.parse.urlencode(params)}"
    payload = _json_get(url, headers=_reddit_headers(access_token, user_agent), timeout_seconds=timeout_seconds)
    children = (((payload.get("data") or {}).get("children") or []))
    return [_reddit_target_from_post((item.get("data") or {}), "reddit_search") for item in children if isinstance(item, dict)]


def _fetch_x_search_recent(
    *,
    query: str,
    limit: int = 25,
    bearer_token: str | None = None,
    timeout_seconds: float = 15.0,
) -> list[HuntTarget]:
    token = bearer_token or os.environ.get("X_BEARER_TOKEN") or os.environ.get("TWITTER_BEARER_TOKEN") or os.environ.get("X_ACCESS_TOKEN")
    if not token:
        raise ValueError("x_search_recent requires bearer_token or X_BEARER_TOKEN/TWITTER_BEARER_TOKEN")
    params = {
        "query": query,
        "max_results": max(10, min(int(limit), 100)),
        "tweet.fields": "author_id,conversation_id,created_at,public_metrics",
        "expansions": "author_id",
        "user.fields": "username,name",
    }
    payload = _json_get(
        f"{X_RECENT_SEARCH_URL}?{urllib.parse.urlencode(params)}",
        headers={"Authorization": f"Bearer {token}"},
        timeout_seconds=timeout_seconds,
    )
    user_map = {
        str(user.get("id")): str(user.get("username") or "").strip() or None
        for user in ((payload.get("includes") or {}).get("users") or [])
        if isinstance(user, dict)
    }
    targets: list[HuntTarget] = []
    for item in payload.get("data") or []:
        if not isinstance(item, dict):
            continue
        tweet_id = str(item.get("id") or "").strip()
        if not tweet_id:
            continue
        author = _clean_author(user_map.get(str(item.get("author_id") or "")))
        metrics = item.get("public_metrics") or {}
        permalink = f"https://x.com/{author or 'i'}/status/{tweet_id}"
        context = {
            "platform": "x",
            "thread_id": str(item.get("conversation_id") or tweet_id),
            "root_author_handle": author,
            "messages": [
                {
                    "message_id": tweet_id,
                    "author_handle": author,
                    "body": str(item.get("text") or ""),
                    "metadata": metrics,
                }
            ],
            "metadata": {"permalink": permalink, **metrics},
        }
        targets.append(
            HuntTarget(
                source_driver="x_search_recent",
                source_item_id=tweet_id,
                platform="x",
                thread_id=str(item.get("conversation_id") or tweet_id),
                reply_to_id=tweet_id,
                author_handle=author,
                subject=None,
                body=str(item.get("text") or ""),
                permalink=permalink,
                context=context,
                metadata={"permalink": permalink, **metrics},
            )
        )
    return targets


def _fetch_jsonl_file(*, file_path: str) -> list[HuntTarget]:
    path = Path(file_path).expanduser().resolve()
    targets: list[HuntTarget] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        item = json.loads(line)
        if not isinstance(item, dict):
            continue
        body = str(item.get("body") or "").strip()
        thread_id = str(item.get("thread_id") or item.get("reply_to_id") or item.get("source_item_id") or "").strip()
        source_item_id = str(item.get("source_item_id") or item.get("reply_to_id") or thread_id).strip()
        if not body or not thread_id or not source_item_id:
            continue
        targets.append(
            HuntTarget(
                source_driver="jsonl_file",
                source_item_id=source_item_id,
                platform=str(item.get("platform") or "reddit"),
                thread_id=thread_id,
                reply_to_id=str(item.get("reply_to_id") or thread_id),
                author_handle=_clean_author(item.get("author_handle")),
                subject=str(item.get("subject") or "").strip() or None,
                body=body,
                permalink=str(item.get("permalink") or "").strip() or None,
                context=item.get("context") if isinstance(item.get("context"), dict) else {
                    "platform": str(item.get("platform") or "reddit"),
                    "thread_id": thread_id,
                    "messages": [{"message_id": str(item.get("reply_to_id") or thread_id), "author_handle": _clean_author(item.get("author_handle")), "body": body}],
                    "metadata": item.get("metadata") if isinstance(item.get("metadata"), dict) else {},
                },
                metadata=item.get("metadata") if isinstance(item.get("metadata"), dict) else {},
            )
        )
    return targets


def fetch_targets(
    source: str,
    *,
    subreddit: str | None = None,
    sort: str = "new",
    query: str | None = None,
    limit: int = 25,
    file_path: str | None = None,
    access_token: str | None = None,
    bearer_token: str | None = None,
    user_agent: str | None = None,
    timeout_seconds: float = 15.0,
) -> list[HuntTarget]:
    logger.info("fetch_targets: source=%s limit=%d", source, limit)
    if source == "jsonl_file":
        if not file_path:
            raise ValueError("jsonl_file source requires file_path")
        targets = _fetch_jsonl_file(file_path=file_path)
        logger.info("fetch_targets: jsonl_file returned %d targets from %s", len(targets), file_path)
        return targets
    if source == "reddit_listing":
        if not subreddit:
            raise ValueError("reddit_listing source requires subreddit")
        token = access_token or os.environ.get("REDDIT_ACCESS_TOKEN")
        targets = _fetch_reddit_listing(subreddit=subreddit, sort=sort, limit=limit, access_token=token, user_agent=user_agent, timeout_seconds=timeout_seconds)
        logger.info("fetch_targets: reddit_listing r/%s returned %d targets", subreddit, len(targets))
        return targets
    if source == "reddit_search":
        if not query:
            raise ValueError("reddit_search source requires query")
        token = access_token or os.environ.get("REDDIT_ACCESS_TOKEN")
        targets = _fetch_reddit_search(query=query, subreddit=subreddit, sort=sort, limit=limit, access_token=token, user_agent=user_agent, timeout_seconds=timeout_seconds)
        logger.info("fetch_targets: reddit_search q=%r returned %d targets", query, len(targets))
        return targets
    if source == "x_search_recent":
        if not query:
            raise ValueError("x_search_recent source requires query")
        targets = _fetch_x_search_recent(query=query, limit=limit, bearer_token=bearer_token, timeout_seconds=timeout_seconds)
        logger.info("fetch_targets: x_search_recent q=%r returned %d targets", query, len(targets))
        return targets
    raise ValueError(f"unknown hunt source: {source}")
