#!/usr/bin/env bash
set -euo pipefail

START_INN="${1:-300000000}"
END_INN="${2:-300999999}"
STEP="${3:-50000}"
MODE="${4:-run}" # run | resume | parse-only

OUT_LINKS="data/orginfo_company_links.csv"
OUT_COMPANIES="data/orginfo_companies.csv"
STATE_FILE="data/local_batch_state.env"

mkdir -p data

save_state() {
  cat > "$STATE_FILE" <<EOF
NEXT_INN=$1
END_INN=$2
STEP=$3
UPDATED_AT=$(date '+%Y-%m-%d %H:%M:%S')
EOF
}

run_parse() {
  echo "[PARSE] links -> companies"
  python3 DataBase/orginfo_parser.py parse-companies \
    --links-csv "$OUT_LINKS" \
    --output-csv "$OUT_COMPANIES" \
    --timeout-ms 30000 \
    --min-delay 1.0 \
    --max-delay 2.0 \
    --verbose
}

if [ "$MODE" = "parse-only" ]; then
  run_parse
  exit 0
fi

if [ "$MODE" = "resume" ]; then
  if [ ! -f "$STATE_FILE" ]; then
    echo "[ERROR] State file not found: $STATE_FILE"
    echo "Start first with mode=run"
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$STATE_FILE"
  current="${NEXT_INN:?}"
  END_INN="${END_INN:?}"
  STEP="${STEP:?}"
  echo "[RESUME] from $current to $END_INN (step=$STEP)"
else
  current="$START_INN"
  save_state "$current" "$END_INN" "$STEP"
fi

while [ "$current" -le "$END_INN" ]; do
  next=$((current + STEP - 1))
  if [ "$next" -gt "$END_INN" ]; then
    next="$END_INN"
  fi

  echo "[BATCH] $current -> $next"
  python3 DataBase/orginfo_parser.py collect-links \
    --start-inn "$current" \
    --end-inn "$next" \
    --output-csv "$OUT_LINKS" \
    --timeout-ms 30000 \
    --min-delay 1.0 \
    --max-delay 2.0 \
    --max-errors 8 \
    --debug-dir data/debug \
    --verbose

  current=$((next + 1))
  save_state "$current" "$END_INN" "$STEP"
done

echo "[DONE] collection finished: next_inn=$current"
run_parse
