#!/usr/bin/env python3
"""Local multiworker runner for orginfo parser.

- Splits INN range into chunks.
- Runs multiple workers in parallel (each worker writes to its own CSV/state).
- Supports resume after stop/failure.
- Recovers progress inside failed chunk from worker logs.
- Merges worker links into one deduplicated CSV.
- Optionally runs parse-companies at the end.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import re
import shutil
import signal
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple


@dataclass
class Config:
    start_inn: int
    end_inn: int
    chunk_size: int
    workers: int
    timeout_ms: int
    min_delay: float
    max_delay: float
    max_errors: int
    max_retries: int
    retry_sleep_sec: int
    resume: bool
    headed: bool
    verbose: bool
    parse_at_end: bool
    parser_script: Path
    work_dir: Path
    merged_links_csv: Path
    merged_companies_csv: Path
    auto_merge_minutes: float
    ops_log: Path
    auto_parse_on_merge: bool
    auto_pipeline_on_merge: bool
    parse_workers: int
    parse_chunk_size: int
    parse_work_dir: Path
    companies_csv: Path
    db_path: Path


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_event(cfg: Config, level: str, action: str, message: str, **extra) -> None:
    payload = {
        "ts": iso_now(),
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


def chunk_range(start: int, end: int, size: int) -> List[Tuple[int, int]]:
    chunks: List[Tuple[int, int]] = []
    current = start
    while current <= end:
        chunk_end = min(current + size - 1, end)
        chunks.append((current, chunk_end))
        current = chunk_end + 1
    return chunks


def assign_chunks(chunks: List[Tuple[int, int]], workers: int) -> List[List[Tuple[int, int]]]:
    assigned: List[List[Tuple[int, int]]] = [[] for _ in range(workers)]
    for idx, chunk in enumerate(chunks):
        assigned[idx % workers].append(chunk)
    return assigned


def write_json(path: Path, payload: dict) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def extract_last_inn_from_stderr(stderr_log: Path, chunk_start: int, chunk_end: int) -> int | None:
    if not stderr_log.exists():
        return None
    text = stderr_log.read_text(encoding="utf-8", errors="ignore")
    nums = []
    for raw in re.findall(r"INN\s+(\d+)", text):
        try:
            inn = int(raw)
        except ValueError:
            continue
        if chunk_start <= inn <= chunk_end:
            nums.append(inn)
    if not nums:
        return None
    return max(nums)


def extract_last_inn_any(stderr_log: Path) -> int | None:
    if not stderr_log.exists():
        return None
    text = stderr_log.read_text(encoding="utf-8", errors="ignore")
    nums = []
    for raw in re.findall(r"INN\s+(\d+)", text):
        try:
            nums.append(int(raw))
        except ValueError:
            continue
    if not nums:
        return None
    return max(nums)


def collect_workers_progress(work_dir: Path) -> tuple[dict[str, int], int | None, dict[str, dict]]:
    progress: dict[str, int] = {}
    details: dict[str, dict] = {}
    max_inn: int | None = None
    for worker_dir in sorted(work_dir.glob("worker_*")):
        stderr_log = worker_dir / "stderr.log"
        state_file = worker_dir / "state.json"

        last_seen_inn = extract_last_inn_any(stderr_log)
        state = read_json(state_file, {})
        last_chunk_end = state.get("last_chunk_end")
        status = str(state.get("status", "unknown"))

        candidates = []
        if isinstance(last_seen_inn, int):
            candidates.append(last_seen_inn)
        if isinstance(last_chunk_end, int):
            candidates.append(last_chunk_end)

        if not candidates:
            details[worker_dir.name] = {
                "status": status,
                "last_seen_inn": None,
                "last_chunk_end": last_chunk_end if isinstance(last_chunk_end, int) else None,
            }
            continue

        best_inn = max(candidates)
        progress[worker_dir.name] = best_inn
        details[worker_dir.name] = {
            "status": status,
            "last_seen_inn": last_seen_inn if isinstance(last_seen_inn, int) else None,
            "last_chunk_end": last_chunk_end if isinstance(last_chunk_end, int) else None,
            "best_inn": best_inn,
        }
        if max_inn is None or best_inn > max_inn:
            max_inn = best_inn
    return progress, max_inn, details


def worker_run(worker_id: int, chunks: List[Tuple[int, int]], cfg: Config) -> int:
    worker_dir = cfg.work_dir / f"worker_{worker_id}"
    ensure_dir(worker_dir)

    state_file = worker_dir / "state.json"
    links_csv = worker_dir / "links.csv"
    stdout_log = worker_dir / "stdout.log"
    stderr_log = worker_dir / "stderr.log"

    state = read_json(
        state_file,
        {
            "next_chunk_idx": 0,
            "status": "new",
            "updated_at": int(time.time()),
        },
    )

    if not cfg.resume:
        state = {
            "next_chunk_idx": 0,
            "status": "new",
            "updated_at": int(time.time()),
        }
        if links_csv.exists():
            links_csv.unlink()

    start_idx = int(state.get("next_chunk_idx", 0))
    if start_idx >= len(chunks):
        state["status"] = "done"
        write_json(state_file, state)
        return 0

    for idx in range(start_idx, len(chunks)):
        chunk_start, chunk_end = chunks[idx]

        run_start = chunk_start
        if cfg.resume and state.get("status") == "failed" and idx == start_idx:
            last_inn = extract_last_inn_from_stderr(stderr_log, chunk_start, chunk_end)
            if last_inn is not None:
                run_start = min(max(last_inn + 1, chunk_start), chunk_end)

        if run_start > chunk_end:
            state.update(
                {
                    "next_chunk_idx": idx + 1,
                    "status": "running",
                    "last_chunk_start": chunk_start,
                    "last_chunk_end": chunk_end,
                    "updated_at": int(time.time()),
                }
            )
            write_json(state_file, state)
            continue

        success = False
        for attempt in range(1, cfg.max_retries + 1):
            cmd = [
                sys.executable,
                str(cfg.parser_script),
                "collect-links",
                "--start-inn",
                str(run_start),
                "--end-inn",
                str(chunk_end),
                "--output-csv",
                str(links_csv),
                "--timeout-ms",
                str(cfg.timeout_ms),
                "--min-delay",
                str(cfg.min_delay),
                "--max-delay",
                str(cfg.max_delay),
                "--max-errors",
                str(cfg.max_errors),
                "--flush-every",
                "1",
                "--debug-dir",
                str(worker_dir / "debug"),
            ]
            if cfg.verbose:
                cmd.append("--verbose")
            if cfg.headed:
                cmd.append("--headed")

            with stdout_log.open("a", encoding="utf-8") as out, stderr_log.open("a", encoding="utf-8") as err:
                out.write(
                    f"\n===== chunk {chunk_start}-{chunk_end} (run {run_start}-{chunk_end}) "
                    f"attempt {attempt} =====\n"
                )
                err.write(
                    f"\n===== chunk {chunk_start}-{chunk_end} (run {run_start}-{chunk_end}) "
                    f"attempt {attempt} =====\n"
                )
                proc = subprocess.run(cmd, stdout=out, stderr=err, text=True)

            if proc.returncode == 0:
                success = True
                break

            if attempt < cfg.max_retries:
                time.sleep(cfg.retry_sleep_sec)

        if not success:
            state.update(
                {
                    "next_chunk_idx": idx,
                    "status": "failed",
                    "failed_chunk_start": chunk_start,
                    "failed_chunk_end": chunk_end,
                    "updated_at": int(time.time()),
                }
            )
            write_json(state_file, state)
            return 1

        state.update(
            {
                "next_chunk_idx": idx + 1,
                "status": "running",
                "last_chunk_start": chunk_start,
                "last_chunk_end": chunk_end,
                "updated_at": int(time.time()),
            }
        )
        write_json(state_file, state)

    state.update(
        {
            "next_chunk_idx": len(chunks),
            "status": "done",
            "updated_at": int(time.time()),
        }
    )
    write_json(state_file, state)
    return 0


def merge_links(work_dir: Path, merged_links_csv: Path) -> int:
    ensure_dir(merged_links_csv.parent)
    seen = set()
    rows = []

    if merged_links_csv.exists():
        with merged_links_csv.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (row.get("company_url") or "").strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                rows.append(
                    {
                        "inn": (row.get("inn") or "").strip(),
                        "company_url": url,
                        "captured_at": (row.get("captured_at") or "").strip(),
                    }
                )

    for links_file in sorted(work_dir.glob("worker_*/links.csv")):
        if not links_file.exists():
            continue
        with links_file.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (row.get("company_url") or "").strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                rows.append(
                    {
                        "inn": (row.get("inn") or "").strip(),
                        "company_url": url,
                        "captured_at": (row.get("captured_at") or "").strip(),
                    }
                )

    with merged_links_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["inn", "company_url", "captured_at"])
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def run_auto_parse(cfg: Config, reason: str) -> int:
    cmd = [
        sys.executable,
        "DataBase/parse_new_links_once.py",
        "--links-csv",
        str(cfg.merged_links_csv),
        "--companies-csv",
        str(cfg.companies_csv),
        "--work-dir",
        str(cfg.parse_work_dir),
        "--workers",
        str(cfg.parse_workers),
        "--chunk-size",
        str(cfg.parse_chunk_size),
        "--timeout-ms",
        str(cfg.timeout_ms),
        "--min-delay",
        str(cfg.min_delay),
        "--max-delay",
        str(cfg.max_delay),
        "--ops-log",
        str(cfg.ops_log),
    ]
    if cfg.verbose:
        cmd.append("--verbose")
    log_event(cfg, "INFO", "auto_parse_trigger", f"reason={reason}")
    rc = subprocess.run(cmd).returncode
    if rc != 0:
        log_event(cfg, "ERROR", "auto_parse_failed", f"reason={reason} rc={rc}")
    else:
        log_event(cfg, "INFO", "auto_parse_done", f"reason={reason}")
    return rc


def run_auto_pipeline(cfg: Config, reason: str) -> int:
    cmd = [
        sys.executable,
        "backend/pipeline.py",
        "--input-csv",
        str(cfg.companies_csv),
        "--links-csv",
        str(cfg.merged_links_csv),
        "--db-path",
        str(cfg.db_path),
    ]
    log_event(cfg, "INFO", "auto_pipeline_trigger", f"reason={reason}")
    rc = subprocess.run(cmd).returncode
    if rc != 0:
        log_event(cfg, "ERROR", "auto_pipeline_failed", f"reason={reason} rc={rc}")
    else:
        log_event(cfg, "INFO", "auto_pipeline_done", f"reason={reason}")
    return rc


def run_parse(cfg: Config) -> int:
    cmd = [
        sys.executable,
        str(cfg.parser_script),
        "parse-companies",
        "--links-csv",
        str(cfg.merged_links_csv),
        "--output-csv",
        str(cfg.merged_companies_csv),
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

    return subprocess.run(cmd).returncode


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Local multiworker for orginfo parser")
    parser.add_argument("--start-inn", type=int, required=True)
    parser.add_argument("--end-inn", type=int, required=True)
    parser.add_argument("--chunk-size", type=int, default=50000)
    parser.add_argument("--workers", type=int, default=4)

    parser.add_argument("--timeout-ms", type=int, default=30000)
    parser.add_argument("--min-delay", type=float, default=1.0)
    parser.add_argument("--max-delay", type=float, default=2.0)
    parser.add_argument("--max-errors", type=int, default=8)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--retry-sleep-sec", type=int, default=10)

    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--no-parse-at-end", action="store_true")
    parser.add_argument("--auto-merge-minutes", type=float, default=5.0)
    parser.add_argument("--ops-log", type=Path, default=Path("data/logs/operations.log"))
    parser.add_argument("--auto-parse-on-merge", action="store_true")
    parser.add_argument("--auto-pipeline-on-merge", action="store_true")
    parser.add_argument("--parse-workers", type=int, default=4)
    parser.add_argument("--parse-chunk-size", type=int, default=100)
    parser.add_argument("--parse-work-dir", type=Path, default=Path("data/auto_parse_delta"))
    parser.add_argument("--companies-csv", type=Path, default=Path("data/orginfo_companies.csv"))
    parser.add_argument("--db-path", type=Path, default=Path("data/makebiz.db"))

    parser.add_argument("--parser-script", type=Path, default=Path("DataBase/orginfo_parser.py"))
    parser.add_argument("--work-dir", type=Path, default=Path("data/local_multi"))
    parser.add_argument("--merged-links-csv", type=Path, default=Path("data/orginfo_company_links.csv"))
    parser.add_argument("--merged-companies-csv", type=Path, default=Path("data/orginfo_companies.csv"))

    args = parser.parse_args()

    if args.end_inn < args.start_inn:
        raise ValueError("end-inn must be >= start-inn")
    if args.workers < 1:
        raise ValueError("workers must be >= 1")
    if args.chunk_size < 1:
        raise ValueError("chunk-size must be >= 1")

    return Config(
        start_inn=args.start_inn,
        end_inn=args.end_inn,
        chunk_size=args.chunk_size,
        workers=args.workers,
        timeout_ms=args.timeout_ms,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        max_errors=args.max_errors,
        max_retries=args.max_retries,
        retry_sleep_sec=args.retry_sleep_sec,
        resume=args.resume,
        headed=args.headed,
        verbose=args.verbose,
        parse_at_end=not args.no_parse_at_end,
        parser_script=args.parser_script,
        work_dir=args.work_dir,
        merged_links_csv=args.merged_links_csv,
        merged_companies_csv=args.merged_companies_csv,
        auto_merge_minutes=args.auto_merge_minutes,
        ops_log=args.ops_log,
        auto_parse_on_merge=args.auto_parse_on_merge,
        auto_pipeline_on_merge=args.auto_pipeline_on_merge,
        parse_workers=args.parse_workers,
        parse_chunk_size=args.parse_chunk_size,
        parse_work_dir=args.parse_work_dir,
        companies_csv=args.companies_csv,
        db_path=args.db_path,
    )


def main() -> int:
    cfg = parse_args()

    if not cfg.resume and cfg.work_dir.exists():
        shutil.rmtree(cfg.work_dir)
    ensure_dir(cfg.work_dir)

    chunks = chunk_range(cfg.start_inn, cfg.end_inn, cfg.chunk_size)
    assigned = assign_chunks(chunks, cfg.workers)

    log_event(
        cfg,
        "INFO",
        "run_start",
        f"range={cfg.start_inn}-{cfg.end_inn} chunks={len(chunks)} workers={cfg.workers} resume={cfg.resume}",
        auto_merge_minutes=cfg.auto_merge_minutes,
        auto_parse_on_merge=cfg.auto_parse_on_merge,
        auto_pipeline_on_merge=cfg.auto_pipeline_on_merge,
    )

    worker_failed = False
    interval_sec = max(0.0, cfg.auto_merge_minutes * 60.0)
    next_merge_at = time.time() + interval_sec if interval_sec > 0 else None
    last_merged_count = 0
    next_progress_at = time.time() + 30.0

    with ProcessPoolExecutor(max_workers=cfg.workers) as pool:
        futures = {
            pool.submit(worker_run, worker_id + 1, assigned[worker_id], cfg): worker_id + 1
            for worker_id in range(cfg.workers)
        }
        pending = set(futures.keys())

        try:
            while pending:
                done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)

                for future in done:
                    wid = futures[future]
                    rc = future.result()
                    if rc != 0:
                        worker_failed = True
                        log_event(cfg, "ERROR", "worker_failed", f"worker {wid} failed")
                    else:
                        log_event(cfg, "INFO", "worker_done", f"worker {wid} done")

                if next_merge_at and time.time() >= next_merge_at:
                    merged_count = merge_links(cfg.work_dir, cfg.merged_links_csv)
                    delta = merged_count - last_merged_count
                    last_merged_count = merged_count
                    workers_progress, global_max_inn, workers_detail = collect_workers_progress(cfg.work_dir)
                    log_event(
                        cfg,
                        "INFO",
                        "auto_merge",
                        f"merged links={merged_count} delta={delta} current_inn={global_max_inn or '-'}",
                        output_csv=str(cfg.merged_links_csv),
                        workers_progress=workers_progress,
                        workers_detail=workers_detail,
                        global_max_inn=global_max_inn,
                    )
                    if delta > 0 and cfg.auto_parse_on_merge:
                        rc_parse = run_auto_parse(cfg, reason="auto_merge")
                        if rc_parse == 0 and cfg.auto_pipeline_on_merge:
                            run_auto_pipeline(cfg, reason="auto_merge")
                    next_merge_at = time.time() + interval_sec

                if time.time() >= next_progress_at:
                    workers_progress, global_max_inn, workers_detail = collect_workers_progress(cfg.work_dir)
                    if workers_progress:
                        log_event(
                            cfg,
                            "INFO",
                            "progress",
                            f"current_inn={global_max_inn or '-'} workers={len(workers_progress)}",
                            workers_progress=workers_progress,
                            workers_detail=workers_detail,
                            global_max_inn=global_max_inn,
                        )
                    next_progress_at = time.time() + 30.0
        except KeyboardInterrupt:
            merged_count = merge_links(cfg.work_dir, cfg.merged_links_csv)
            workers_progress, global_max_inn, workers_detail = collect_workers_progress(cfg.work_dir)
            log_event(
                cfg,
                "WARN",
                "interrupted",
                "Interrupted by user. workers will stop; resume with --resume",
                merged_links=merged_count,
                workers_progress=workers_progress,
                workers_detail=workers_detail,
                global_max_inn=global_max_inn,
            )
            pool.shutdown(wait=False, cancel_futures=True)
            return 130

    merged_count = merge_links(cfg.work_dir, cfg.merged_links_csv)
    workers_progress, global_max_inn, workers_detail = collect_workers_progress(cfg.work_dir)
    log_event(
        cfg,
        "INFO",
        "final_merge",
        f"merged links={merged_count} current_inn={global_max_inn or '-'}",
        output_csv=str(cfg.merged_links_csv),
        workers_progress=workers_progress,
        workers_detail=workers_detail,
        global_max_inn=global_max_inn,
    )
    if cfg.auto_parse_on_merge:
        rc_parse = run_auto_parse(cfg, reason="final_merge")
        if rc_parse == 0 and cfg.auto_pipeline_on_merge:
            run_auto_pipeline(cfg, reason="final_merge")

    if worker_failed:
        log_event(cfg, "WARN", "run_finish", "some workers failed. fix issue and rerun with --resume")
        return 1

    if cfg.parse_at_end:
        log_event(cfg, "INFO", "parse_start", "parse-companies started", output_csv=str(cfg.merged_companies_csv))
        rc = run_parse(cfg)
        if rc != 0:
            log_event(cfg, "ERROR", "parse_failed", "parse-companies failed", rc=rc)
            return rc
        log_event(cfg, "INFO", "parse_done", "parse-companies completed", output_csv=str(cfg.merged_companies_csv))

    log_event(cfg, "INFO", "run_finish", "run completed successfully")

    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.default_int_handler)
    raise SystemExit(main())
