#!/usr/bin/env bash
set -euo pipefail

# MakeBiz full cycle:
# 1) collect links (multiworker, auto-merge)
# 2) parse company pages in parallel
# 3) rebuild SQLite DB pipeline

START_INN="${START_INN:-300000000}"
END_INN="${END_INN:-300999999}"

COLLECT_WORKERS="${COLLECT_WORKERS:-4}"
COLLECT_CHUNK_SIZE="${COLLECT_CHUNK_SIZE:-50000}"
COLLECT_TIMEOUT_MS="${COLLECT_TIMEOUT_MS:-30000}"
COLLECT_MIN_DELAY="${COLLECT_MIN_DELAY:-1.0}"
COLLECT_MAX_DELAY="${COLLECT_MAX_DELAY:-2.0}"
COLLECT_MAX_ERRORS="${COLLECT_MAX_ERRORS:-8}"
COLLECT_MAX_RETRIES="${COLLECT_MAX_RETRIES:-2}"
COLLECT_RETRY_SLEEP_SEC="${COLLECT_RETRY_SLEEP_SEC:-10}"
AUTO_MERGE_MINUTES="${AUTO_MERGE_MINUTES:-5}"

PARSE_WORKERS="${PARSE_WORKERS:-4}"
PARSE_CHUNK_SIZE="${PARSE_CHUNK_SIZE:-200}"
PARSE_TIMEOUT_MS="${PARSE_TIMEOUT_MS:-30000}"
PARSE_MIN_DELAY="${PARSE_MIN_DELAY:-1.0}"
PARSE_MAX_DELAY="${PARSE_MAX_DELAY:-2.0}"

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "[STEP 1/3] collect-links multiworker"
python3 DataBase/run_local_multiworker.py \
  --start-inn "$START_INN" \
  --end-inn "$END_INN" \
  --chunk-size "$COLLECT_CHUNK_SIZE" \
  --workers "$COLLECT_WORKERS" \
  --timeout-ms "$COLLECT_TIMEOUT_MS" \
  --min-delay "$COLLECT_MIN_DELAY" \
  --max-delay "$COLLECT_MAX_DELAY" \
  --max-errors "$COLLECT_MAX_ERRORS" \
  --max-retries "$COLLECT_MAX_RETRIES" \
  --retry-sleep-sec "$COLLECT_RETRY_SLEEP_SEC" \
  --auto-merge-minutes "$AUTO_MERGE_MINUTES" \
  --ops-log data/logs/operations.log \
  --resume \
  --verbose \
  --no-parse-at-end

echo "[STEP 2/3] parse-companies multiworker"
python3 DataBase/parse_companies_multiworker.py \
  --links-csv data/orginfo_company_links.csv \
  --output-csv data/orginfo_companies.csv \
  --work-dir data/parse_multi \
  --workers "$PARSE_WORKERS" \
  --chunk-size "$PARSE_CHUNK_SIZE" \
  --timeout-ms "$PARSE_TIMEOUT_MS" \
  --min-delay "$PARSE_MIN_DELAY" \
  --max-delay "$PARSE_MAX_DELAY" \
  --ops-log data/logs/operations.log \
  --verbose

echo "[STEP 3/3] pipeline to SQLite DB"
python3 backend/pipeline.py \
  --input-csv data/orginfo_companies.csv \
  --links-csv data/orginfo_company_links.csv \
  --db-path data/makebiz.db

echo "[DONE] Full autoparse cycle finished"
echo "CSV links: data/orginfo_company_links.csv"
echo "CSV companies: data/orginfo_companies.csv"
echo "DB: data/makebiz.db"
