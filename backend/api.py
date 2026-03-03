from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

try:
    from .db import get_conn, init_schema
    from .scoring import score_company
except ImportError:
    from db import get_conn, init_schema
    from scoring import score_company

app = FastAPI(title='MakeBiz API', version='0.1.0')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

DB_FILE = Path('data/makebiz.db')
OPS_LOG_FILE = Path('data/logs/operations.log')
with closing(get_conn(DB_FILE)) as _conn:
    init_schema(_conn)


class RFQCreate(BaseModel):
    title: str
    company_name: str
    budget_uzs: int = 0
    deadline: str = '-'
    details: str = '-'


class OfferCreate(BaseModel):
    rfq_id: int
    company_id: int
    offer_amount_uzs: int = 0
    status: str = 'NEW'
    note: str = '-'


TEXT_FIELDS = {
    'source_url',
    'company_name',
    'short_name',
    'inn',
    'registration_date',
    'activity_status',
    'registration_authority',
    'thsht',
    'dbibt',
    'ifut',
    'email',
    'phone',
    'address',
    'region',
    'district',
    'category',
    'tax_committee',
    'large_taxpayer',
    'director',
    'founders',
}
INT_FIELDS = {'charter_capital_uzs', 'employees_count', 'branch_count'}
EDITABLE_FIELDS = TEXT_FIELDS | INT_FIELDS


def norm_text(value: Any) -> str:
    text = '' if value is None else str(value).strip()
    return text if text else '-'


def norm_int(value: Any) -> int:
    if value is None:
        return 0
    digits = ''.join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else 0


def parse_ifut(ifut_raw: str) -> tuple[str, str]:
    value = (ifut_raw or '').strip()
    if not value or value == '-':
        return '-', '-'
    if ' - ' in value:
        code, name = value.split(' - ', 1)
        return code.strip(), name.strip() or '-'
    return value, '-'


def parse_thsht(thsht_raw: str) -> tuple[str, str]:
    value = (thsht_raw or '').strip()
    if not value or value == '-':
        return '-', '-'
    if ' - ' in value:
        code, name = value.split(' - ', 1)
        return code.strip(), name.strip() or '-'
    return value, '-'


def log_event(level: str, action: str, message: str, **extra) -> None:
    payload = {
        'ts': datetime.now(timezone.utc).isoformat(),
        'level': level.upper(),
        'action': action,
        'message': message,
    }
    if extra:
        payload['extra'] = extra
    OPS_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OPS_LOG_FILE.open('a', encoding='utf-8') as f:
        f.write(json.dumps(payload, ensure_ascii=False) + '\n')


def read_recent_logs(limit: int) -> list[dict]:
    if not OPS_LOG_FILE.exists():
        return []
    lines = OPS_LOG_FILE.read_text(encoding='utf-8', errors='ignore').splitlines()
    out = []
    for line in lines[-limit:]:
        try:
            out.append(json.loads(line))
        except Exception:
            out.append({'ts': '-', 'level': 'INFO', 'action': 'raw', 'message': line})
    return out


def update_company_score(conn, company_id: int) -> None:
    row = conn.execute('SELECT * FROM companies WHERE id = ?', (company_id,)).fetchone()
    if not row:
        return
    company = dict(row)
    scored = score_company(company)

    conn.execute(
        '''
        UPDATE companies
        SET score = ?, score_label = ?, profile_completeness = ?, contact_valid = ?, score_explain = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        ''',
        (
            int(scored['score']),
            scored['score_label'],
            float(scored['profile_completeness']),
            int(scored['contact_valid']),
            scored['score_explain'],
            company_id,
        ),
    )

    explain = json.loads(scored['score_explain'])
    conn.execute(
        '''
        INSERT INTO company_scores (
          company_id, total_score, completeness_score, contacts_score,
          activity_score, profile_depth_score, freshness_score, explain_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(company_id) DO UPDATE SET
          total_score=excluded.total_score,
          completeness_score=excluded.completeness_score,
          contacts_score=excluded.contacts_score,
          activity_score=excluded.activity_score,
          profile_depth_score=excluded.profile_depth_score,
          freshness_score=excluded.freshness_score,
          explain_json=excluded.explain_json,
          updated_at=CURRENT_TIMESTAMP
        ''',
        (
            company_id,
            int(scored['score']),
            int(explain.get('completeness_score', 0)),
            int(explain.get('contacts_score', 0)),
            int(explain.get('activity_score', 0)),
            int(explain.get('profile_depth_score', 0)),
            int(explain.get('freshness_score', 0)),
            scored['score_explain'],
        ),
    )


@app.get('/api/health')
def health():
    return {'ok': True}


@app.get('/api/companies')
def companies(
    q: str = '',
    region: str = '',
    sector: str = '',
    oked: str = '',
    stability: str = '',
    legal_form: str = '',
    thsht: str = '',
    min_score: int = 0,
    order_by: str = Query('score_desc'),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    where = [
        "1=1",
        "company_name NOT LIKE 'Unknown company %'",
        "LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')",
        "inn != '-'",
        "inn != ''",
    ]
    params = []

    if q:
        where.append('(company_name LIKE ? OR company_name_raw LIKE ? OR inn LIKE ?)')
        params.extend([f'%{q}%', f'%{q}%', f'%{q}%'])
    if region:
        where.append('region = ?')
        params.append(region)
    if sector:
        where.append('ifut LIKE ?')
        params.append(f'{sector}%')
    if oked:
        where.append('ifut LIKE ?')
        params.append(f'{oked}%')
    if stability:
        where.append('category = ?')
        params.append(stability)
    if legal_form:
        where.append('legal_form = ?')
        params.append(legal_form)
    if thsht:
        where.append('(thsht LIKE ? OR thsht LIKE ?)')
        params.extend([f'{thsht}%', f'%{thsht}%'])
    if min_score:
        where.append('score >= ?')
        params.append(min_score)

    order_map = {
        'score_desc': 'score DESC, id DESC',
        'score_asc': 'score ASC, id DESC',
        'name_asc': 'company_name ASC, id DESC',
        'name_desc': 'company_name DESC, id DESC',
        'updated_desc': 'updated_at DESC, id DESC',
    }
    order_sql = order_map.get(order_by, order_map['score_desc'])

    page_params = [*params, limit, offset]
    query = f'''
      SELECT *
      FROM companies
      WHERE {' AND '.join(where)}
      ORDER BY {order_sql}
      LIMIT ? OFFSET ?
    '''

    with closing(get_conn(DB_FILE)) as conn:
        total = conn.execute(
            f"SELECT COUNT(*) c FROM companies WHERE {' AND '.join(where)}",
            params,
        ).fetchone()['c']
        rows = []
        for r in conn.execute(query, page_params).fetchall():
            item = dict(r)
            oked_code, oked_name = parse_ifut(item.get('ifut', '-'))
            item['oked_code'] = oked_code
            item['oked_name'] = oked_name
            thsht_code, thsht_text = parse_thsht(item.get('thsht', '-'))
            item['thsht_code'] = thsht_code
            item['thsht_text'] = thsht_text
            item['stability_rating'] = item.get('category', '-')
            rows.append(item)
    return {'items': rows, 'count': len(rows), 'total': total, 'limit': limit, 'offset': offset}


@app.get('/api/companies/{company_id}')
def company_detail(company_id: int):
    with closing(get_conn(DB_FILE)) as conn:
        row = conn.execute('SELECT * FROM companies WHERE id = ?', (company_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail='Company not found')

        rel = conn.execute(
            '''
            SELECT cr.relation_type, cr.weight, c2.id AS related_id, c2.company_name AS related_company
            FROM company_relations cr
            JOIN companies c2 ON c2.id = cr.to_company_id
            WHERE cr.from_company_id = ?
            ORDER BY cr.weight DESC
            LIMIT 30
            ''',
            (company_id,),
        ).fetchall()

    payload = dict(row)
    oked_code, oked_name = parse_ifut(payload.get('ifut', '-'))
    payload['oked_code'] = oked_code
    payload['oked_name'] = oked_name
    thsht_code, thsht_text = parse_thsht(payload.get('thsht', '-'))
    payload['thsht_code'] = thsht_code
    payload['thsht_text'] = thsht_text
    payload['stability_rating'] = payload.get('category', '-')
    try:
        payload['score_explain'] = json.loads(payload.get('score_explain') or '{}')
    except Exception:
        payload['score_explain'] = {}
    return {'company': payload, 'relations': [dict(x) for x in rel]}


@app.get('/api/meta/filters')
def meta_filters():
    with closing(get_conn(DB_FILE)) as conn:
        regions = [
            r['region']
            for r in conn.execute(
                """
                SELECT region
                FROM companies
                WHERE region != '-'
                  AND company_name NOT LIKE 'Unknown company %'
                  AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
                  AND inn != '-' AND inn != ''
                GROUP BY region
                ORDER BY region ASC
                """
            ).fetchall()
        ]
        stabilities = [
            r['category']
            for r in conn.execute(
                """
                SELECT category
                FROM companies
                WHERE category != '-'
                  AND company_name NOT LIKE 'Unknown company %'
                  AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
                  AND inn != '-' AND inn != ''
                GROUP BY category
                ORDER BY COUNT(*) DESC, category ASC
                """
            ).fetchall()
        ]
        legal_forms = [
            r['legal_form']
            for r in conn.execute(
                """
                SELECT legal_form
                FROM companies
                WHERE legal_form != '-'
                  AND company_name NOT LIKE 'Unknown company %'
                  AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
                  AND inn != '-' AND inn != ''
                GROUP BY legal_form
                ORDER BY COUNT(*) DESC, legal_form ASC
                LIMIT 100
                """
            ).fetchall()
        ]
        ifut_rows = conn.execute(
            """
            SELECT ifut, COUNT(*) c
            FROM companies
            WHERE ifut != '-'
              AND company_name NOT LIKE 'Unknown company %'
              AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
              AND inn != '-' AND inn != ''
            GROUP BY ifut
            ORDER BY c DESC
            """
        ).fetchall()
        thsht_rows = conn.execute(
            """
            SELECT thsht, COUNT(*) c
            FROM companies
            WHERE thsht != '-'
              AND company_name NOT LIKE 'Unknown company %'
              AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
              AND inn != '-' AND inn != ''
            GROUP BY thsht
            ORDER BY c DESC
            LIMIT 200
            """
        ).fetchall()

    oked_items = []
    for row in ifut_rows:
        code, name = parse_ifut(row['ifut'])
        if code == '-':
            continue
        major = code[:2] if len(code) >= 2 and code[:2].isdigit() else '00'
        oked_items.append(
            {
                'major_code': major,
                'code': code,
                'name': name,
                'full': row['ifut'],
                'count': row['c'],
            }
        )

    groups_map: dict[str, dict] = {}
    for item in oked_items:
        key = item['major_code']
        group = groups_map.setdefault(
            key,
            {
                'major_code': key,
                'label': f'{key}xx',
                'count': 0,
                'subcategories': [],
            },
        )
        group['count'] += int(item['count'])
        group['subcategories'].append(item)

    groups = sorted(
        groups_map.values(),
        key=lambda x: (-x['count'], x['major_code']),
    )
    for g in groups:
        g['subcategories'] = sorted(
            g['subcategories'],
            key=lambda x: (-int(x['count']), x['code']),
        )

    return {
        'regions': regions,
        'stabilities': stabilities,
        'legal_forms': legal_forms,
        'thsht_items': [dict(x) for x in thsht_rows],
        'oked_groups': groups,
    }


@app.put('/api/companies/{company_id}')
def update_company(company_id: int, payload: dict[str, Any]):
    incoming = {k: v for k, v in (payload or {}).items() if k in EDITABLE_FIELDS}
    if not incoming:
        raise HTTPException(status_code=400, detail='No editable fields provided')

    assigns = []
    params = []
    for key, value in incoming.items():
        assigns.append(f'{key} = ?')
        if key in INT_FIELDS:
            params.append(norm_int(value))
        else:
            params.append(norm_text(value))

    with closing(get_conn(DB_FILE)) as conn:
        existing = conn.execute('SELECT id FROM companies WHERE id = ?', (company_id,)).fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail='Company not found')

        params.append(company_id)
        conn.execute(
            f'''
            UPDATE companies
            SET {', '.join(assigns)}, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''',
            params,
        )
        update_company_score(conn, company_id)
        conn.commit()
        row = conn.execute('SELECT * FROM companies WHERE id = ?', (company_id,)).fetchone()

    result = dict(row)
    try:
        result['score_explain'] = json.loads(result.get('score_explain') or '{}')
    except Exception:
        result['score_explain'] = {}
    log_event('INFO', 'company_update', f'company_id={company_id} updated', fields=sorted(incoming.keys()))
    return {'ok': True, 'company': result}


@app.post('/api/rfq')
def create_rfq(payload: RFQCreate):
    with closing(get_conn(DB_FILE)) as conn:
        conn.execute(
            '''
            INSERT INTO rfq(title, company_name, budget_uzs, deadline, details, status)
            VALUES (?, ?, ?, ?, ?, 'OPEN')
            ''',
            (payload.title, payload.company_name, payload.budget_uzs, payload.deadline, payload.details),
        )
        conn.commit()
        rfq_id = conn.execute('SELECT last_insert_rowid() id').fetchone()['id']
    log_event('INFO', 'rfq_create', f'rfq_id={rfq_id} created', company_name=payload.company_name)
    return {'id': rfq_id, 'status': 'OPEN'}


@app.get('/api/rfq')
def list_rfq(limit: int = Query(100, ge=1, le=500)):
    with closing(get_conn(DB_FILE)) as conn:
        rows = [dict(r) for r in conn.execute('SELECT * FROM rfq ORDER BY id DESC LIMIT ?', (limit,)).fetchall()]
    return {'items': rows}


@app.post('/api/offers')
def create_offer(payload: OfferCreate):
    with closing(get_conn(DB_FILE)) as conn:
        rfq = conn.execute('SELECT id FROM rfq WHERE id = ?', (payload.rfq_id,)).fetchone()
        if not rfq:
            raise HTTPException(status_code=404, detail='RFQ not found')

        company = conn.execute('SELECT id FROM companies WHERE id = ?', (payload.company_id,)).fetchone()
        if not company:
            raise HTTPException(status_code=404, detail='Company not found')

        conn.execute(
            '''
            INSERT INTO rfq_offers(rfq_id, company_id, offer_amount_uzs, status, note)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (payload.rfq_id, payload.company_id, payload.offer_amount_uzs, payload.status, payload.note),
        )
        conn.commit()
        offer_id = conn.execute('SELECT last_insert_rowid() id').fetchone()['id']

        # relation as market interaction
        conn.execute(
            '''
            INSERT OR IGNORE INTO company_relations(from_company_id, to_company_id, relation_type, weight, source)
            VALUES (?, ?, 'rfq_interaction', 0.7, 'rfq')
            ''',
            (payload.company_id, payload.company_id),
        )
        conn.commit()

    log_event(
        'INFO',
        'offer_create',
        f'offer_id={offer_id} created',
        rfq_id=payload.rfq_id,
        company_id=payload.company_id,
    )
    return {'id': offer_id, 'status': payload.status}


@app.get('/api/dashboard/summary')
def dashboard_summary():
    with closing(get_conn(DB_FILE)) as conn:
        total = conn.execute(
            """
            SELECT COUNT(*) c
            FROM companies
            WHERE company_name NOT LIKE 'Unknown company %'
              AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
              AND inn != '-' AND inn != ''
            """
        ).fetchone()['c']
        rfq_open = conn.execute("SELECT COUNT(*) c FROM rfq WHERE status = 'OPEN'").fetchone()['c']
        regions = conn.execute("SELECT COUNT(DISTINCT region) c FROM companies WHERE region != '-'").fetchone()['c']
        avg_score = conn.execute('SELECT ROUND(AVG(score), 2) v FROM companies').fetchone()['v']
        dedup_conflicts = conn.execute('SELECT COUNT(*) c FROM dedup_conflicts').fetchone()['c']
        relations = conn.execute('SELECT COUNT(*) c FROM company_relations').fetchone()['c']
        legal_forms = conn.execute(
            """
            SELECT COUNT(DISTINCT legal_form) c
            FROM companies
            WHERE legal_form != '-'
              AND company_name NOT LIKE 'Unknown company %'
              AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
              AND inn != '-' AND inn != ''
            """
        ).fetchone()['c']

    return {
        'total_companies': total,
        'open_rfq': rfq_open,
        'regions': regions,
        'avg_score': avg_score or 0,
        'dedup_conflicts': dedup_conflicts,
        'relations': relations,
        'legal_forms': legal_forms or 0,
    }


@app.get('/api/dashboard/scoring-distribution')
def scoring_distribution():
    with closing(get_conn(DB_FILE)) as conn:
        rows = conn.execute(
            '''
            SELECT
              SUM(CASE WHEN score >= 80 THEN 1 ELSE 0 END) AS high,
              SUM(CASE WHEN score BETWEEN 65 AND 79 THEN 1 ELSE 0 END) AS medium,
              SUM(CASE WHEN score < 65 THEN 1 ELSE 0 END) AS low
            FROM companies
            WHERE company_name NOT LIKE 'Unknown company %'
              AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
              AND inn != '-' AND inn != ''
            '''
        ).fetchone()
    return {'high': rows['high'] or 0, 'medium': rows['medium'] or 0, 'low': rows['low'] or 0}


@app.get('/api/dashboard/regions')
def regions_breakdown(limit: int = Query(20, ge=1, le=100)):
    with closing(get_conn(DB_FILE)) as conn:
        rows = conn.execute(
            '''
            SELECT region, COUNT(*) AS companies, ROUND(AVG(score), 2) AS avg_score
            FROM companies
            WHERE region != '-'
              AND company_name NOT LIKE 'Unknown company %'
              AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
              AND inn != '-' AND inn != ''
            GROUP BY region
            ORDER BY companies DESC
            LIMIT ?
            ''',
            (limit,),
        ).fetchall()
    return {'items': [dict(r) for r in rows]}


@app.get('/api/dashboard/data-quality')
def data_quality():
    with closing(get_conn(DB_FILE)) as conn:
        total = conn.execute(
            """
            SELECT COUNT(*) c
            FROM companies
            WHERE company_name NOT LIKE 'Unknown company %'
              AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
              AND inn != '-' AND inn != ''
            """
        ).fetchone()['c']
        if total == 0:
            return {
                'total': 0,
                'with_contacts_pct': 0,
                'with_region_pct': 0,
                'with_category_pct': 0,
                'conflicts': 0,
            }

        with_contacts = conn.execute(
            """
            SELECT COUNT(*) c FROM companies
            WHERE (phone != '-' OR email != '-')
              AND company_name NOT LIKE 'Unknown company %'
              AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
              AND inn != '-' AND inn != ''
            """
        ).fetchone()['c']
        with_region = conn.execute(
            """
            SELECT COUNT(*) c FROM companies
            WHERE region != '-'
              AND company_name NOT LIKE 'Unknown company %'
              AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
              AND inn != '-' AND inn != ''
            """
        ).fetchone()['c']
        with_category = conn.execute(
            """
            SELECT COUNT(*) c FROM companies
            WHERE category != '-'
              AND company_name NOT LIKE 'Unknown company %'
              AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
              AND inn != '-' AND inn != ''
            """
        ).fetchone()['c']
        conflicts = conn.execute('SELECT COUNT(*) c FROM dedup_conflicts').fetchone()['c']

    return {
        'total': total,
        'with_contacts_pct': round(with_contacts * 100 / total, 2),
        'with_region_pct': round(with_region * 100 / total, 2),
        'with_category_pct': round(with_category * 100 / total, 2),
        'conflicts': conflicts,
    }


@app.get('/api/dashboard/deep')
def dashboard_deep():
    with closing(get_conn(DB_FILE)) as conn:
        summary = dashboard_summary()
        scoring = scoring_distribution()
        quality = data_quality()

        categories = [
            dict(r)
            for r in conn.execute(
                '''
                SELECT category, COUNT(*) AS companies, ROUND(AVG(score), 2) AS avg_score
                FROM companies
                WHERE category != '-'
                  AND company_name NOT LIKE 'Unknown company %'
                  AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
                  AND inn != '-' AND inn != ''
                GROUP BY category
                ORDER BY companies DESC
                LIMIT 20
                '''
            ).fetchall()
        ]

        statuses = [
            dict(r)
            for r in conn.execute(
                '''
                SELECT activity_status, COUNT(*) AS companies
                FROM companies
                WHERE activity_status != '-'
                  AND company_name NOT LIKE 'Unknown company %'
                  AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
                  AND inn != '-' AND inn != ''
                GROUP BY activity_status
                ORDER BY companies DESC
                LIMIT 20
                '''
            ).fetchall()
        ]

        missing_fields = [
            {
                'field': 'phone',
                'missing': conn.execute("SELECT COUNT(*) c FROM companies WHERE phone='-' OR phone=''").fetchone()['c'],
            },
            {
                'field': 'email',
                'missing': conn.execute("SELECT COUNT(*) c FROM companies WHERE email='-' OR email=''").fetchone()['c'],
            },
            {
                'field': 'region',
                'missing': conn.execute("SELECT COUNT(*) c FROM companies WHERE region='-' OR region=''").fetchone()['c'],
            },
            {
                'field': 'category',
                'missing': conn.execute("SELECT COUNT(*) c FROM companies WHERE category='-' OR category=''").fetchone()['c'],
            },
            {
                'field': 'director',
                'missing': conn.execute("SELECT COUNT(*) c FROM companies WHERE director='-' OR director=''").fetchone()['c'],
            },
            {
                'field': 'address',
                'missing': conn.execute("SELECT COUNT(*) c FROM companies WHERE address='-' OR address=''").fetchone()['c'],
            },
        ]

        top_companies = [
            dict(r)
            for r in conn.execute(
                '''
                SELECT id, company_name, inn, region, category, score, updated_at
                FROM companies
                WHERE company_name NOT LIKE 'Unknown company %'
                  AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
                  AND inn != '-' AND inn != ''
                ORDER BY score DESC, updated_at DESC
                LIMIT 15
                '''
            ).fetchall()
        ]

        weakest_companies = [
            dict(r)
            for r in conn.execute(
                '''
                SELECT id, company_name, inn, region, category, score, updated_at
                FROM companies
                WHERE company_name NOT LIKE 'Unknown company %'
                  AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
                  AND inn != '-' AND inn != ''
                ORDER BY score ASC, updated_at DESC
                LIMIT 15
                '''
            ).fetchall()
        ]

        recent_updates = [
            dict(r)
            for r in conn.execute(
                '''
                SELECT id, company_name, inn, region, category, score, updated_at
                FROM companies
                WHERE company_name NOT LIKE 'Unknown company %'
                  AND LOWER(company_name) NOT IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
                  AND inn != '-' AND inn != ''
                ORDER BY updated_at DESC, id DESC
                LIMIT 30
                '''
            ).fetchall()
        ]

        open_conflicts = [
            dict(r)
            for r in conn.execute(
                '''
                SELECT dc.id, dc.reason, dc.confidence, dc.status,
                       a.company_name AS company_a, b.company_name AS company_b
                FROM dedup_conflicts dc
                JOIN companies a ON a.id = dc.company_a_id
                JOIN companies b ON b.id = dc.company_b_id
                ORDER BY dc.id DESC
                LIMIT 30
                '''
            ).fetchall()
        ]

    return {
        'summary': summary,
        'scoring': scoring,
        'quality': quality,
        'categories': categories,
        'statuses': statuses,
        'missing_fields': missing_fields,
        'top_companies': top_companies,
        'weakest_companies': weakest_companies,
        'recent_updates': recent_updates,
        'open_conflicts': open_conflicts,
    }


@app.get('/api/dashboard/pipeline-runs')
def pipeline_runs(limit: int = Query(20, ge=1, le=100)):
    with closing(get_conn(DB_FILE)) as conn:
        rows = conn.execute('SELECT * FROM source_runs ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
    items = []
    for r in rows:
        item = dict(r)
        if item.get('notes'):
            try:
                item['notes'] = json.loads(item['notes'])
            except Exception:
                pass
        items.append(item)
    return {'items': items}


@app.get('/api/logs')
def api_logs(limit: int = Query(200, ge=1, le=2000)):
    return {'items': read_recent_logs(limit)}


# Serve the website as static files from /web
web_root = Path('website').resolve()
if web_root.exists():
    app.mount('/web', StaticFiles(directory=str(web_root), html=True), name='web')


@app.get('/')
def root():
    return RedirectResponse(url='/web/dashboard.html')
