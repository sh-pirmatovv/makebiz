#!/usr/bin/env python3
"""Parse only new links (not yet present in companies CSV), then merge.

Flow:
1) Read links CSV + companies CSV
2) Build delta links by source_url
3) Parse only delta links via parse_companies_multiworker
4) Merge parsed rows into companies CSV
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

COMPANY_COLUMNS = [
    "source_url",
    "company_name",
    "company_name_raw",
    "legal_form",
    "short_name",
    "inn",
    "registration_date",
    "activity_status",
    "registration_authority",
    "thsht",
    "dbibt",
    "ifut",
    "charter_capital_uzs",
    "email",
    "phone",
    "address",
    "region",
    "district",
    "category",
    "tax_committee",
    "large_taxpayer",
    "director",
    "founders",
    "employees_count",
    "branch_count",
]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_event(ops_log: Path, level: str, action: str, message: str, **extra) -> None:
    payload = {
        "ts": now_iso(),
        "level": level.upper(),
        "action": action,
        "message": message,
    }
    if extra:
        payload["extra"] = extra
    ensure_dir(ops_log.parent)
    with ops_log.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    print(f"[{payload['level']}] {action}: {message}", flush=True)


def read_companies_urls(companies_csv: Path) -> set[str]:
    if not companies_csv.exists():
        return set()
    out = set()
    with companies_csv.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            u = (row.get("source_url") or "").strip()
            if u:
                out.add(u)
    return out


def read_links_rows(links_csv: Path) -> list[dict[str, str]]:
    rows = []
    with links_csv.open("r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            u = (r.get("company_url") or "").strip()
            if not u:
                continue
            rows.append(
                {
                    "inn": (r.get("inn") or "").strip(),
                    "company_url": u,
                    "captured_at": (r.get("captured_at") or "").strip(),
                }
            )
    return rows


def write_links(path: Path, rows: list[dict[str, str]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["inn", "company_url", "captured_at"])
        w.writeheader()
        w.writerows(rows)


def read_companies_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            rows.append({c: (r.get(c) or "-").strip() or "-" for c in COMPANY_COLUMNS})
    return rows


def write_companies_rows(path: Path, rows: list[dict[str, str]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COMPANY_COLUMNS)
        w.writeheader()
        w.writerows(rows)


def merge_companies(existing: list[dict[str, str]], new_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_url = {}
    for row in existing:
        u = (row.get("source_url") or "").strip()
        if not u:
            continue
        by_url[u] = row
    for row in new_rows:
        u = (row.get("source_url") or "").strip()
        if not u:
            continue
        by_url[u] = row
    return list(by_url.values())


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Parse only new links and merge into companies CSV")
    p.add_argument("--links-csv", type=Path, default=Path("data/orginfo_company_links.csv"))
    p.add_argument("--companies-csv", type=Path, default=Path("data/orginfo_companies.csv"))
    p.add_argument("--work-dir", type=Path, default=Path("data/auto_parse_delta"))
    p.add_argument("--parser-script", type=Path, default=Path("DataBase/orginfo_parser.py"))
    p.add_argument("--multi-script", type=Path, default=Path("DataBase/parse_companies_multiworker.py"))
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--chunk-size", type=int, default=100)
    p.add_argument("--timeout-ms", type=int, default=30000)
    p.add_argument("--min-delay", type=float, default=1.0)
    p.add_argument("--max-delay", type=float, default=2.0)
    p.add_argument("--ops-log", type=Path, default=Path("data/logs/operations.log"))
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if not args.links_csv.exists():
        log_event(args.ops_log, "WARN", "auto_parse_skip", f"links csv missing: {args.links_csv}")
        return 1

    links = read_links_rows(args.links_csv)
    existing_urls = read_companies_urls(args.companies_csv)
    delta = [r for r in links if r["company_url"] not in existing_urls]

    if not delta:
        log_event(args.ops_log, "INFO", "auto_parse_skip", "no new links to parse", links_total=len(links))
        return 0

    ensure_dir(args.work_dir)
    delta_links_csv = args.work_dir / "delta.links.csv"
    delta_companies_csv = args.work_dir / "delta.companies.csv"
    write_links(delta_links_csv, delta)

    log_event(
        args.ops_log,
        "INFO",
        "auto_parse_start",
        f"new_links={len(delta)}",
        workers=args.workers,
        chunk_size=args.chunk_size,
    )

    cmd = [
        sys.executable,
        str(args.multi_script),
        "--links-csv",
        str(delta_links_csv),
        "--output-csv",
        str(delta_companies_csv),
        "--work-dir",
        str(args.work_dir / "parts"),
        "--parser-script",
        str(args.parser_script),
        "--workers",
        str(args.workers),
        "--chunk-size",
        str(args.chunk_size),
        "--timeout-ms",
        str(args.timeout_ms),
        "--min-delay",
        str(args.min_delay),
        "--max-delay",
        str(args.max_delay),
        "--ops-log",
        str(args.ops_log),
    ]
    if args.verbose:
        cmd.append("--verbose")

    rc = subprocess.run(cmd).returncode
    if rc != 0:
        log_event(args.ops_log, "ERROR", "auto_parse_failed", f"parse multi failed rc={rc}")
        return rc

    existing_rows = read_companies_rows(args.companies_csv)
    new_rows = read_companies_rows(delta_companies_csv)
    merged_rows = merge_companies(existing_rows, new_rows)
    write_companies_rows(args.companies_csv, merged_rows)

    log_event(
        args.ops_log,
        "INFO",
        "auto_parse_merge_done",
        f"companies_total={len(merged_rows)} parsed_new={len(new_rows)}",
        companies_csv=str(args.companies_csv),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
