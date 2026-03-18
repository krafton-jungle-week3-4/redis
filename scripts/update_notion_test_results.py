#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from urllib.parse import urlencode
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_NOTION_PAGE_ID = "327bd214-dd7e-80aa-a930-c2ff985f64a3"
NOTION_VERSION = "2026-03-11"


def normalize_page_id(page_id: str) -> str:
    cleaned = "".join(char for char in page_id if char.isalnum())
    if len(cleaned) != 32:
        raise ValueError("NOTION_PAGE_ID must be a 32 character id or UUID with hyphens")
    return (
        f"{cleaned[0:8]}-{cleaned[8:12]}-{cleaned[12:16]}-"
        f"{cleaned[16:20]}-{cleaned[20:32]}"
    )


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def expected_case_ids() -> list[str]:
    try:
        from scripts.run_qa_suite import QA_CASES
    except ModuleNotFoundError:
        from run_qa_suite import QA_CASES

    return [case["id"] for case in QA_CASES]


def validate_report(report: dict) -> None:
    cases = report.get("cases")
    if not isinstance(cases, list):
        raise RuntimeError("QA report is missing a valid 'cases' list")

    expected_ids = expected_case_ids()
    expected_id_set = set(expected_ids)

    seen_ids: list[str] = []
    seen_id_set: set[str] = set()
    duplicate_ids: list[str] = []
    unexpected_ids: list[str] = []

    for case in cases:
        if not isinstance(case, dict) or not isinstance(case.get("id"), str):
            raise RuntimeError("QA report contains a case without a valid string 'id'")
        case_id = case["id"]
        if case_id in seen_id_set and case_id not in duplicate_ids:
            duplicate_ids.append(case_id)
        seen_ids.append(case_id)
        seen_id_set.add(case_id)
        if case_id not in expected_id_set and case_id not in unexpected_ids:
            unexpected_ids.append(case_id)

    missing_ids = [case_id for case_id in expected_ids if case_id not in seen_id_set]
    if missing_ids or duplicate_ids or unexpected_ids:
        problems: list[str] = []
        if missing_ids:
            problems.append(f"missing={', '.join(missing_ids)}")
        if duplicate_ids:
            problems.append(f"duplicate={', '.join(duplicate_ids)}")
        if unexpected_ids:
            problems.append(f"unexpected={', '.join(unexpected_ids)}")
        raise RuntimeError(f"QA report is incomplete or inconsistent: {'; '.join(problems)}")

    if len(cases) != len(expected_ids):
        raise RuntimeError(
            f"QA report case count mismatch: expected {len(expected_ids)}, got {len(cases)}"
        )


def build_run_url() -> str | None:
    server_url = os.getenv("GITHUB_SERVER_URL")
    repository = os.getenv("GITHUB_REPOSITORY")
    run_id = os.getenv("GITHUB_RUN_ID")
    if not server_url or not repository or not run_id:
        return None
    return f"{server_url}/{repository}/actions/runs/{run_id}"


def rich_text(content: str, *, bold: bool = False) -> dict:
    return {
        "type": "text",
        "text": {"content": content},
        "annotations": {
            "bold": bold,
            "italic": False,
            "strikethrough": False,
            "underline": False,
            "code": False,
            "color": "default",
        },
    }


def heading_block(content: str) -> dict:
    return {
        "object": "block",
        "type": "heading_2",
        "heading_2": {"rich_text": [rich_text(content)]},
    }


def paragraph_block(content: str, *, bold: bool = False) -> dict:
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": [rich_text(content, bold=bold)]},
    }


def bullet_block(content: str) -> dict:
    return {
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {"rich_text": [rich_text(content)]},
    }


def divider_block() -> dict:
    return {"object": "block", "type": "divider", "divider": {}}


def table_row(cells: list[str], *, bold: bool = False) -> dict:
    return {
        "object": "block",
        "type": "table_row",
        "table_row": {
            "cells": [[rich_text(cell, bold=bold)] for cell in cells],
        },
    }


def table_block(rows: list[list[str]]) -> dict:
    header = table_row(["ID", "항목", "상태", "상세 내용", "우선순위"], bold=True)
    data_rows = [table_row(row) for row in rows]
    return {
        "object": "block",
        "type": "table",
        "table": {
            "table_width": 5,
            "has_column_header": True,
            "has_row_header": False,
            "children": [header, *data_rows],
        },
    }


def build_children(report: dict) -> list[dict]:
    run_url = build_run_url()
    rows = [
        [case["id"], case["title"], case["status_label"], case["detail"], case["priority"]]
        for case in report["cases"]
    ]

    children = [
        heading_block(report["title"]),
        paragraph_block("💡 요약 정보", bold=True),
        bullet_block(f"생성 시각: {report['generated_at']}"),
        bullet_block(f"실행 결과: {report['summary_text']}"),
        bullet_block(f"소요 시간: {report['duration_sec']}초"),
        bullet_block(f"명령어: {report['command']}"),
    ]

    if run_url is not None:
        children.append(paragraph_block(f"GitHub Actions 실행 링크: {run_url}"))

    children.extend(
        [
            table_block(rows),
            divider_block(),
        ]
    )
    return children


def append_blocks(page_id: str, token: str, children: list[dict], dry_run: bool) -> None:
    payload = {"children": children}
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


def fetch_block_children(page_id: str, token: str) -> list[dict]:
    children: list[dict] = []
    next_cursor: str | None = None

    while True:
        params = {"page_size": 100}
        if next_cursor is not None:
            params["start_cursor"] = next_cursor
        url = f"https://api.notion.com/v1/blocks/{page_id}/children?{urlencode(params)}"
        request = Request(
            url=url,
            headers={
                "Authorization": f"Bearer {token}",
                "Notion-Version": NOTION_VERSION,
            },
            method="GET",
        )

        try:
            with urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Failed to read existing Notion blocks ({exc.code}): {body}") from exc
        except URLError as exc:
            raise RuntimeError(f"Failed to reach Notion API: {exc}") from exc

        children.extend(payload.get("results", []))
        if not payload.get("has_more"):
            return children
        next_cursor = payload.get("next_cursor")


def delete_block(block_id: str, token: str) -> None:
    request = Request(
        url=f"https://api.notion.com/v1/blocks/{block_id}",
        headers={
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
        },
        method="DELETE",
    )

    try:
        with urlopen(request, timeout=30) as response:
            response.read()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Failed to delete Notion block {block_id} ({exc.code}): {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Failed to reach Notion API: {exc}") from exc


def clear_page_children(page_id: str, token: str, dry_run: bool) -> None:
    children = fetch_block_children(page_id, token)
    if dry_run:
        print(json.dumps({"delete_block_ids": [child["id"] for child in children]}, ensure_ascii=False, indent=2))
        return

    remaining = children
    for _ in range(10):
        for child in remaining:
            delete_block(child["id"], token)

        time.sleep(0.5)
        remaining = fetch_block_children(page_id, token)
        if not remaining:
            return

    unresolved_ids = ", ".join(child["id"] for child in remaining)
    raise RuntimeError(f"Notion page content could not be fully cleared. Remaining block ids: {unresolved_ids}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Append QA status report to a Notion page.")
    parser.add_argument("report_file", help="Path to the generated QA report JSON file")
    parser.add_argument("--dry-run", action="store_true", help="Print the Notion payload instead of sending it")
    args = parser.parse_args()

    report_path = Path(args.report_file)
    report = read_json(report_path)
    validate_report(report)

    page_id = normalize_page_id(os.getenv("NOTION_PAGE_ID") or DEFAULT_NOTION_PAGE_ID)
    token = os.getenv("NOTION_TOKEN", "")

    if not args.dry_run and not token:
        raise RuntimeError("NOTION_TOKEN is required to update the Notion page")

    children = build_children(report)
    if not args.dry_run:
        clear_page_children(page_id, token, dry_run=False)
    append_blocks(page_id, token, children, args.dry_run)
    print(f"Updated Notion page {page_id} with QA report {report_path.name}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
