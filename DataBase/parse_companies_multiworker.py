#!/usr/bin/env python3
"""Parallel parser for company pages from links CSV.

- Splits links CSV into chunk CSV files.
- Runs multiple parse-companies workers in parallel.
- Merges all worker outputs into one deduplicated companies CSV.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

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


@dataclass
class Config:
    links_csv: Path
    output_csv: Path
    parser_script: Path
    work_dir: Path
    workers: int
    chunk_size: int
    timeout_ms: int
    min_delay: float
    max_delay: float
    headed: bool
    verbose: bool
    max_companies: int | None
    ops_log: Path


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_event(cfg: Config, level: str, action: str, message: str, **extra) -> None:
    payload = {
        "ts": now_iso(),
        "level": level.upper(),
        "action": action,
        "message": message,
    }
    if extra:
        payload["extra"] = extra

    ensure_dir(cfg.ops_log.parent)
    with cfg.ops_log.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    print(f"[{payload['level']}] {action}: {message}", flush=True)


def read_links(path: Path, max_companies: int | None) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Links CSV not found: {path}")

    seen = set()
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("company_url") or "").strip()
            inn = (row.get("inn") or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            rows.append({"inn": inn, "company_url": url, "captured_at": (row.get("captured_at") or "").strip()})
            if max_companies and len(rows) >= max_companies:
                break
    return rows


def chunked(items: list[dict[str, str]], size: int) -> Iterable[list[dict[str, str]]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def write_links_chunk(path: Path, rows: list[dict[str, str]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["inn", "company_url", "captured_at"])
        writer.writeheader()
        writer.writerows(rows)


def run_worker(worker_id: int, links_chunk_csv: Path, output_csv: Path, cfg: Config) -> int:
    out_log = cfg.work_dir / f"worker_{worker_id}" / "stdout.log"
    err_log = cfg.work_dir / f"worker_{worker_id}" / "stderr.log"
    ensure_dir(out_log.parent)

    cmd = [
        sys.executable,
        str(cfg.parser_script),
        "parse-companies",
        "--links-csv",
        str(links_chunk_csv),
        "--output-csv",
        str(output_csv),
        "--timeout-ms",
        str(cfg.timeout_ms),
        "--min-delay",
        str(cfg.min_delay),
        "--max-delay",
        str(cfg.max_delay),
    ]
    if cfg.verbose:
        cmd.append("--verbose")
    if cfg.headed:
        cmd.append("--headed")

    with out_log.open("a", encoding="utf-8") as out, err_log.open("a", encoding="utf-8") as err:
        out.write(f"\n===== worker {worker_id} start =====\n")
        err.write(f"\n===== worker {worker_id} start =====\n")
        rc = subprocess.run(cmd, stdout=out, stderr=err).returncode
    return rc


def merge_companies_csv(parts: list[Path], output_csv: Path) -> int:
    ensure_dir(output_csv.parent)
    seen = set()
    merged: list[dict[str, str]] = []

    for part in parts:
        if not part.exists():
            continue
        with part.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (row.get("source_url") or "").strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                merged.append({col: (row.get(col) or "-").strip() or "-" for col in COMPANY_COLUMNS})

    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COMPANY_COLUMNS)
        writer.writeheader()
        writer.writerows(merged)

    return len(merged)


def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Parallel parse-companies runner")
    p.add_argument("--links-csv", type=Path, default=Path("data/orginfo_company_links.csv"))
    p.add_argument("--output-csv", type=Path, default=Path("data/orginfo_companies.csv"))
    p.add_argument("--parser-script", type=Path, default=Path("DataBase/orginfo_parser.py"))
    p.add_argument("--work-dir", type=Path, default=Path("data/parse_multi"))
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--chunk-size", type=int, default=200)
    p.add_argument("--timeout-ms", type=int, default=30000)
    p.add_argument("--min-delay", type=float, default=1.0)
    p.add_argument("--max-delay", type=float, default=2.0)
    p.add_argument("--max-companies", type=int, default=None)
    p.add_argument("--ops-log", type=Path, default=Path("data/logs/operations.log"))
    p.add_argument("--headed", action="store_true")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    if args.workers < 1:
        raise ValueError("workers must be >= 1")
    if args.chunk_size < 1:
        raise ValueError("chunk-size must be >= 1")

    return Config(
        links_csv=args.links_csv,
        output_csv=args.output_csv,
        parser_script=args.parser_script,
        work_dir=args.work_dir,
        workers=args.workers,
        chunk_size=args.chunk_size,
        timeout_ms=args.timeout_ms,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        headed=args.headed,
        verbose=args.verbose,
        max_companies=args.max_companies,
        ops_log=args.ops_log,
    )


def main() -> int:
    cfg = parse_args()
    ensure_dir(cfg.work_dir)

    links = read_links(cfg.links_csv, cfg.max_companies)
    if not links:
        log_event(cfg, "WARN", "parse_multi_empty", "No links to parse", links_csv=str(cfg.links_csv))
        return 1

    chunks = list(chunked(links, cfg.chunk_size))
    log_event(
        cfg,
        "INFO",
        "parse_multi_start",
        f"links={len(links)} chunks={len(chunks)} workers={cfg.workers}",
        links_csv=str(cfg.links_csv),
        output_csv=str(cfg.output_csv),
    )

    worker_inputs: list[tuple[int, Path, Path]] = []
    for idx, rows in enumerate(chunks, start=1):
        in_csv = cfg.work_dir / f"chunk_{idx:05d}.links.csv"
        out_csv = cfg.work_dir / f"chunk_{idx:05d}.companies.csv"
        write_links_chunk(in_csv, rows)
        worker_inputs.append((idx, in_csv, out_csv))

    failed = False
    try:
        executor = ProcessPoolExecutor(max_workers=cfg.workers)
        pool_kind = "process"
    except Exception:
        executor = ThreadPoolExecutor(max_workers=cfg.workers)
        pool_kind = "thread_fallback"
        log_event(cfg, "WARN", "parse_pool_fallback", "ProcessPool unavailable, using ThreadPool fallback")

    with executor as pool:
        futures = {
            pool.submit(run_worker, chunk_id, in_csv, out_csv, cfg): (chunk_id, out_csv)
            for chunk_id, in_csv, out_csv in worker_inputs
        }

        for fut in as_completed(futures):
            chunk_id, out_csv = futures[fut]
            rc = fut.result()
            if rc != 0:
                failed = True
                log_event(
                    cfg,
                    "ERROR",
                    "parse_worker_failed",
                    f"chunk={chunk_id} failed",
                    chunk_id=chunk_id,
                    pool=pool_kind,
                )
            else:
                log_event(
                    cfg,
                    "INFO",
                    "parse_worker_done",
                    f"chunk={chunk_id} done",
                    chunk_id=chunk_id,
                    out_csv=str(out_csv),
                    pool=pool_kind,
                )

    part_files = [out_csv for _, _, out_csv in worker_inputs]
    merged = merge_companies_csv(part_files, cfg.output_csv)
    log_event(cfg, "INFO", "parse_multi_merge", f"merged companies={merged}", output_csv=str(cfg.output_csv))

    if failed:
        log_event(cfg, "WARN", "parse_multi_finish", "completed with worker errors")
        return 1

    log_event(cfg, "INFO", "parse_multi_finish", "completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
