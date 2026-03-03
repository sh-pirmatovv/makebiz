# MakeBiz Backend (Local)

## Install

```bash
python3 -m pip install -r backend/requirements-backend.txt
```

## Build DB from parsed CSV

```bash
python3 backend/pipeline.py --input-csv data/orginfo_companies.csv --db-path data/makebiz.db
```

With links fallback (recommended while full company parsing is still running):

```bash
python3 backend/pipeline.py --input-csv data/orginfo_companies.csv --links-csv data/orginfo_company_links.csv --db-path data/makebiz.db
```

## Auto-sync DB while parser runs

```bash
python3 backend/sync_loop.py --input-csv data/orginfo_companies.csv --links-csv data/orginfo_company_links.csv --db-path data/makebiz.db --interval-sec 120
```

## Run API + web

```bash
python3 -m uvicorn backend.api:app --reload --host 0.0.0.0 --port 9000
```

Open:
- API docs: http://localhost:9000/docs
- Main web: http://localhost:9000/web/index.html
- Dashboard: http://localhost:9000/web/dashboard.html

## Key API

- `GET /api/companies`
- `GET /api/companies/{id}`
- `POST /api/rfq`
- `GET /api/rfq`
- `POST /api/offers`
- `GET /api/dashboard/summary`
- `GET /api/dashboard/scoring-distribution`
- `GET /api/dashboard/regions`
- `GET /api/dashboard/data-quality`
- `GET /api/dashboard/pipeline-runs`
