#!/usr/bin/env python3
"""Recover company links from worker stderr logs.

Looks for lines like:
  INN 300000013 -> https://orginfo.uz/uz/organization/...
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

LOG_GLOB = "data/local_multi/worker_*/stderr.log"
OUT_CSV = Path("data/orginfo_company_links.csv")

PATTERN = re.compile(r"INN\s+(\d+)\s+->\s+(https?://\S+)")


def main() -> int:
    rows = []
    seen = set()

    for log in sorted(Path("data/local_multi").glob("worker_*/stderr.log")):
        text = log.read_text(encoding="utf-8", errors="ignore")
        for inn, url in PATTERN.findall(text):
            url = url.strip()
            if url in seen:
                continue
            seen.add(url)
            rows.append({"inn": inn, "company_url": url, "captured_at": "recovered_from_log"})

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["inn", "company_url", "captured_at"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"recovered {len(rows)} links -> {OUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
