#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_NOTION_PAGE_ID = "327bd214-dd7e-80aa-a930-c2ff985f64a3"
NOTION_VERSION = "2026-03-11"
MAX_RICH_TEXT_CHARS = 1800


def normalize_page_id(page_id: str) -> str:
    cleaned = "".join(char for char in page_id if char.isalnum())
    if len(cleaned) != 32:
        raise ValueError("NOTION_PAGE_ID must be a 32 character id or UUID with hyphens")
    return (
        f"{cleaned[0:8]}-{cleaned[8:12]}-{cleaned[12:16]}-"
        f"{cleaned[16:20]}-{cleaned[20:32]}"
    )


def read_results(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def chunk_text(text: str, size: int = MAX_RICH_TEXT_CHARS) -> list[str]:
    if not text:
        return [""]
    return [text[index:index + size] for index in range(0, len(text), size)]


def text_block(content: str) -> dict:
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [{"type": "text", "text": {"content": content}}],
        },
    }


def bullet_block(content: str) -> dict:
    return {
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {
            "rich_text": [{"type": "text", "text": {"content": content}}],
        },
    }


def heading_block(content: str) -> dict:
    return {
        "object": "block",
        "type": "heading_2",
        "heading_2": {
            "rich_text": [{"type": "text", "text": {"content": content}}],
        },
    }


def code_block(content: str) -> dict:
    return {
        "object": "block",
        "type": "code",
        "code": {
            "rich_text": [{"type": "text", "text": {"content": content}}],
            "language": "plain text",
        },
    }


def divider_block() -> dict:
    return {"object": "block", "type": "divider", "divider": {}}


def build_run_url() -> str | None:
    server_url = os.getenv("GITHUB_SERVER_URL")
    repository = os.getenv("GITHUB_REPOSITORY")
    run_id = os.getenv("GITHUB_RUN_ID")
    if not server_url or not repository or not run_id:
        return None
    return f"{server_url}/{repository}/actions/runs/{run_id}"


def build_children(result_text: str, status_text: str) -> list[dict]:
    branch = os.getenv("GITHUB_REF_NAME", "unknown")
    repository = os.getenv("GITHUB_REPOSITORY", "unknown")
    commit_sha = os.getenv("GITHUB_SHA", "unknown")
    event_name = os.getenv("GITHUB_EVENT_NAME", "unknown")
    run_url = build_run_url()
    timestamp = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    children = [
        heading_block(f"자동 테스트 결과 - {timestamp}"),
        bullet_block(f"저장소: {repository}"),
        bullet_block(f"브랜치: {branch}"),
        bullet_block(f"이벤트: {event_name}"),
        bullet_block(f"커밋: {commit_sha[:7]}"),
        bullet_block(f"결과: {status_text}"),
    ]

    if run_url is not None:
        children.append(text_block(f"GitHub Actions 실행 링크: {run_url}"))

    children.append(text_block("원본 테스트 출력:"))
    for chunk in chunk_text(result_text):
        children.append(code_block(chunk))

    children.append(divider_block())
    return children


def append_blocks(page_id: str, token: str, children: list[dict], dry_run: bool) -> None:
    payload = {
        "children": children,
        "position": {"type": "start"},
    }
    if dry_run:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    request = Request(
        url=f"https://api.notion.com/v1/blocks/{page_id}/children",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        },
        method="PATCH",
    )

    try:
        with urlopen(request, timeout=30) as response:
            response.read()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if exc.code == 404:
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                payload = {}
            if payload.get("code") == "object_not_found":
                message = payload.get("message", "The requested page or block was not found.")
                raise RuntimeError(
                    "Notion target page was not found or is not shared with the integration. "
                    f"page_id={page_id}. "
                    "Check NOTION_PAGE_ID and share the page with the integration in "
                    "Notion via Share/Add connections. "
                    f"Notion said: {message}"
                ) from exc
        raise RuntimeError(f"Notion API error {exc.code}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Failed to reach Notion API: {exc}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Append latest test results to a Notion page.")
    parser.add_argument("results_file", help="Path to the captured test output file")
    parser.add_argument("--dry-run", action="store_true", help="Print the Notion payload instead of sending it")
    args = parser.parse_args()

    results_path = Path(args.results_file)
    result_text = read_results(results_path)

    exit_code = int(os.getenv("TEST_EXIT_CODE", "0"))
    status_text = "PASS" if exit_code == 0 else f"FAIL (exit code {exit_code})"

    page_id = normalize_page_id(os.getenv("NOTION_PAGE_ID") or DEFAULT_NOTION_PAGE_ID)
    token = os.getenv("NOTION_TOKEN", "")

    if not args.dry_run and not token:
        raise RuntimeError("NOTION_TOKEN is required to update the Notion page")

    children = build_children(result_text, status_text)
    append_blocks(page_id, token, children, args.dry_run)
    print(f"Updated Notion page {page_id} with latest test results ({status_text}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
