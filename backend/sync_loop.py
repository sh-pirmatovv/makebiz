from __future__ import annotations

import argparse
import time
from pathlib import Path

from pipeline import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description='Continuously sync CSV -> DB while parser is running')
    parser.add_argument('--input-csv', type=Path, default=Path('data/orginfo_companies.csv'))
    parser.add_argument('--links-csv', type=Path, default=Path('data/orginfo_company_links.csv'))
    parser.add_argument('--db-path', type=Path, default=Path('data/makebiz.db'))
    parser.add_argument('--interval-sec', type=int, default=120)
    args = parser.parse_args()

    last_stamp = None
    print(f'[SYNC] watching {args.input_csv} + {args.links_csv} every {args.interval_sec}s')

    while True:
        try:
            stamp = (
                args.input_csv.stat().st_mtime if args.input_csv.exists() else 0,
                args.links_csv.stat().st_mtime if args.links_csv.exists() else 0,
            )
            if stamp != last_stamp:
                run_pipeline(args.input_csv, args.db_path, args.links_csv)
                print('[SYNC] pipeline updated')
                last_stamp = stamp
        except Exception as exc:
            print(f'[SYNC] error: {exc}')

        time.sleep(max(5, args.interval_sec))


if __name__ == '__main__':
    raise SystemExit(main())
