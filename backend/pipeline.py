from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

try:
    from .db import get_conn, init_schema
    from .scoring import score_company
except ImportError:
    from db import get_conn, init_schema
    from scoring import score_company


COMPANY_FIELDS = [
    'source_url', 'company_name', 'company_name_raw', 'legal_form', 'short_name', 'inn', 'registration_date', 'activity_status',
    'registration_authority', 'thsht', 'dbibt', 'ifut', 'charter_capital_uzs', 'email', 'phone',
    'address', 'region', 'district', 'category', 'tax_committee', 'large_taxpayer', 'director',
    'founders', 'employees_count', 'branch_count'
]


def clean(v: str, default: str = '-') -> str:
    v = (v or '').strip()
    if not v or v in {'—', '–', '-'}:
        return default
    return v


def to_int(v: str, default: int = 0) -> int:
    digits = ''.join(ch for ch in (v or '') if ch.isdigit())
    return int(digits) if digits else default


def split_company_name_and_legal_form(name: str) -> tuple[str, str, str]:
    raw = clean(name)
    if raw == '-':
        return '-', '-', '-'
    for ql, qr in [('\"', '\"'), ('«', '»'), ('“', '”')]:
        if ql in raw and qr in raw:
            left = raw.find(ql)
            right = raw.find(qr, left + 1)
            if left != -1 and right != -1 and right > left + 1:
                core = clean(raw[left + 1 : right])
                outside = (raw[:left] + ' ' + raw[right + 1 :]).strip(' ,.-')
                legal = clean(outside)
                return (core if core != '-' else raw), legal, raw
    return raw, '-', raw


def read_companies(csv_path: Path) -> list[dict]:
    rows = []
    with csv_path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            item = {k: clean(row.get(k, '-')) for k in COMPANY_FIELDS}
            if item.get('company_name_raw', '-') == '-' and item.get('legal_form', '-') == '-':
                cname, legal, raw = split_company_name_and_legal_form(item.get('company_name', '-'))
                item['company_name'] = cname
                item['legal_form'] = legal
                item['company_name_raw'] = raw
            item['charter_capital_uzs'] = to_int(row.get('charter_capital_uzs', '0'))
            item['employees_count'] = to_int(row.get('employees_count', '0'))
            item['branch_count'] = to_int(row.get('branch_count', '0'))
            rows.append(item)
    return rows


def upsert_companies(conn, companies: list[dict]) -> int:
    q = '''
    INSERT INTO companies (
      source_url, company_name, company_name_raw, legal_form, short_name, inn, registration_date, activity_status,
      registration_authority, thsht, dbibt, ifut, charter_capital_uzs, email, phone,
      address, region, district, category, tax_committee, large_taxpayer, director,
      founders, employees_count, branch_count, score, score_label, profile_completeness,
      contact_valid, score_explain, updated_at
    ) VALUES (
      :source_url, :company_name, :company_name_raw, :legal_form, :short_name, :inn, :registration_date, :activity_status,
      :registration_authority, :thsht, :dbibt, :ifut, :charter_capital_uzs, :email, :phone,
      :address, :region, :district, :category, :tax_committee, :large_taxpayer, :director,
      :founders, :employees_count, :branch_count, :score, :score_label, :profile_completeness,
      :contact_valid, :score_explain, CURRENT_TIMESTAMP
    )
    ON CONFLICT(source_url) DO UPDATE SET
      company_name=excluded.company_name,
      company_name_raw=excluded.company_name_raw,
      legal_form=excluded.legal_form,
      short_name=excluded.short_name,
      inn=excluded.inn,
      registration_date=excluded.registration_date,
      activity_status=excluded.activity_status,
      registration_authority=excluded.registration_authority,
      thsht=excluded.thsht,
      dbibt=excluded.dbibt,
      ifut=excluded.ifut,
      charter_capital_uzs=excluded.charter_capital_uzs,
      email=excluded.email,
      phone=excluded.phone,
      address=excluded.address,
      region=excluded.region,
      district=excluded.district,
      category=excluded.category,
      tax_committee=excluded.tax_committee,
      large_taxpayer=excluded.large_taxpayer,
      director=excluded.director,
      founders=excluded.founders,
      employees_count=excluded.employees_count,
      branch_count=excluded.branch_count,
      score=excluded.score,
      score_label=excluded.score_label,
      profile_completeness=excluded.profile_completeness,
      contact_valid=excluded.contact_valid,
      score_explain=excluded.score_explain,
      updated_at=CURRENT_TIMESTAMP
    '''

    payload = []
    for row in companies:
        s = score_company(row)
        payload.append({**row, **s})

    conn.executemany(q, payload)
    conn.commit()

    score_rows = conn.execute('SELECT id, score, score_explain FROM companies').fetchall()
    params = []
    for r in score_rows:
        ex = json.loads(r['score_explain'] or '{}')
        params.append(
            (
                r['id'],
                r['score'],
                int(ex.get('completeness_score', 0)),
                int(ex.get('contacts_score', 0)),
                int(ex.get('activity_score', 0)),
                int(ex.get('profile_depth_score', 0)),
                int(ex.get('freshness_score', 0)),
                r['score_explain'],
            )
        )

    conn.executemany(
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
        params,
    )
    conn.commit()
    return len(payload)


def detect_dedup_conflicts(conn) -> int:
    conn.execute('DELETE FROM dedup_conflicts')

    rows = conn.execute(
        '''
        SELECT id, company_name, inn, phone, region
        FROM companies
        WHERE phone != '-' AND phone != ''
        ORDER BY phone
        '''
    ).fetchall()

    by_key = defaultdict(list)
    for r in rows:
        key = (r['phone'], r['region'])
        by_key[key].append(r)

    conflicts = []
    for _key, items in by_key.items():
        if len(items) < 2:
            continue
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                a, b = items[i], items[j]
                if a['inn'] != b['inn']:
                    conflicts.append((a['id'], b['id'], 'same_phone_region_diff_inn', 0.82, 'OPEN'))

    conn.executemany(
        '''
        INSERT OR IGNORE INTO dedup_conflicts(company_a_id, company_b_id, reason, confidence, status)
        VALUES (?, ?, ?, ?, ?)
        ''',
        conflicts,
    )
    conn.commit()
    return len(conflicts)


def build_relations(conn) -> int:
    conn.execute('DELETE FROM company_relations')

    rows = conn.execute(
        '''
        SELECT id, region, category, score
        FROM companies
        '''
    ).fetchall()

    by_region = defaultdict(list)
    by_category = defaultdict(list)
    for r in rows:
        by_region[r['region']].append(r)
        by_category[r['category']].append(r)

    edges = []

    def pairwise(group, rel, weight):
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                if a['id'] == b['id']:
                    continue
                edges.append((a['id'], b['id'], rel, weight, 'system'))
                edges.append((b['id'], a['id'], rel, weight, 'system'))

    for group in by_region.values():
        if len(group) <= 1:
            continue
        pairwise(group[:40], 'same_region', 0.35)

    for group in by_category.values():
        if len(group) <= 1:
            continue
        pairwise(group[:40], 'same_sector', 0.45)

    high = [r for r in rows if int(r['score']) >= 80]
    pairwise(high[:60], 'high_trust_peer', 0.62)

    conn.executemany(
        '''
        INSERT OR IGNORE INTO company_relations(from_company_id, to_company_id, relation_type, weight, source)
        VALUES (?, ?, ?, ?, ?)
        ''',
        edges,
    )
    conn.commit()
    return len(edges)


def data_quality(conn) -> dict:
    total = conn.execute('SELECT COUNT(*) c FROM companies').fetchone()['c']
    if total == 0:
        return {
            'total_companies': 0,
            'with_contacts_pct': 0,
            'with_region_pct': 0,
            'with_category_pct': 0,
            'dedup_conflicts': 0,
        }

    with_contacts = conn.execute(
        "SELECT COUNT(*) c FROM companies WHERE phone != '-' OR email != '-'"
    ).fetchone()['c']
    with_region = conn.execute("SELECT COUNT(*) c FROM companies WHERE region != '-'").fetchone()['c']
    with_category = conn.execute("SELECT COUNT(*) c FROM companies WHERE category != '-'").fetchone()['c']
    conflicts = conn.execute('SELECT COUNT(*) c FROM dedup_conflicts').fetchone()['c']

    return {
        'total_companies': total,
        'with_contacts_pct': round(with_contacts * 100 / total, 2),
        'with_region_pct': round(with_region * 100 / total, 2),
        'with_category_pct': round(with_category * 100 / total, 2),
        'dedup_conflicts': conflicts,
    }


def read_links_csv(links_csv: Path) -> list[dict]:
    if not links_csv.exists():
        return []
    rows = []
    with links_csv.open('r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            inn = clean(r.get('inn', '-'))
            url = clean(r.get('company_url', '-'))
            if url == '-':
                continue
            rows.append(
                {
                    'source_url': url,
                    'company_name': f'Unknown company {inn}',
                    'company_name_raw': '-',
                    'legal_form': '-',
                    'short_name': '-',
                    'inn': inn,
                    'registration_date': '-',
                    'activity_status': '-',
                    'registration_authority': '-',
                    'thsht': '-',
                    'dbibt': '-',
                    'ifut': '-',
                    'charter_capital_uzs': 0,
                    'email': '-',
                    'phone': '-',
                    'address': '-',
                    'region': '-',
                    'district': '-',
                    'category': '-',
                    'tax_committee': '-',
                    'large_taxpayer': '-',
                    'director': '-',
                    'founders': '-',
                    'employees_count': 0,
                    'branch_count': 0,
                }
            )
    return rows


def merge_companies_with_links(companies: list[dict], links_rows: list[dict]) -> list[dict]:
    by_url = {row['source_url']: row for row in companies if row.get('source_url')}
    for link_row in links_rows:
        url = link_row.get('source_url')
        if not url or url not in by_url:
            continue
        if by_url[url].get('inn', '-') in {'', '-'} and link_row.get('inn', '-') not in {'', '-'}:
            by_url[url]['inn'] = link_row['inn']
    return list(by_url.values())


def normalize_company_names_in_db(conn) -> int:
    rows = conn.execute('SELECT id, company_name, company_name_raw, legal_form FROM companies').fetchall()
    updates = []
    for r in rows:
        cname, legal, raw = split_company_name_and_legal_form(r['company_name'])
        old_raw = clean(r['company_name_raw']) if r['company_name_raw'] is not None else '-'
        old_legal = clean(r['legal_form']) if r['legal_form'] is not None else '-'
        if cname != r['company_name'] or raw != old_raw or legal != old_legal:
            updates.append((cname, raw, legal, r['id']))
    if updates:
        conn.executemany(
            '''
            UPDATE companies
            SET company_name = ?, company_name_raw = ?, legal_form = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''',
            updates,
        )
        conn.commit()
    return len(updates)


def cleanup_bad_rows(conn) -> int:
    cur = conn.execute(
        '''
        DELETE FROM companies
        WHERE company_name LIKE 'Unknown company %'
           OR LOWER(company_name) IN ('500', '404', '403', 'internal server error', 'bad gateway', 'gateway timeout', 'error')
           OR inn = '-' OR inn = '' OR inn IS NULL
        '''
    )
    deleted = cur.rowcount or 0
    conn.commit()
    return deleted


def run_pipeline(input_csv: Path, db_path: Path, links_csv: Path | None = None) -> None:
    companies = read_companies(input_csv)
    if links_csv:
        companies = merge_companies_with_links(companies, read_links_csv(links_csv))
    conn = get_conn(db_path)
    init_schema(conn)

    started = datetime.utcnow().isoformat()
    conn.execute(
        'INSERT INTO source_runs(source_name, started_at, status, rows_in) VALUES (?, ?, ?, ?)',
        ('orginfo_companies_csv', started, 'RUNNING', len(companies)),
    )
    run_id = conn.execute('SELECT last_insert_rowid() AS id').fetchone()['id']
    conn.commit()

    inserted = upsert_companies(conn, companies)
    normalized_names = normalize_company_names_in_db(conn)
    deleted_bad = cleanup_bad_rows(conn)
    conflicts = detect_dedup_conflicts(conn)
    relations = build_relations(conn)
    quality = data_quality(conn)

    notes = {
        'bad_rows_deleted': deleted_bad,
        'company_names_normalized': normalized_names,
        'conflicts_generated': conflicts,
        'relations_generated': relations,
        'quality': quality,
    }

    conn.execute(
        'UPDATE source_runs SET finished_at=?, status=?, rows_out=?, notes=? WHERE id=?',
        (datetime.utcnow().isoformat(), 'DONE', inserted, json.dumps(notes, ensure_ascii=False), run_id),
    )
    conn.commit()
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load CSV into MakeBiz DB + scoring + dedup + relations')
    parser.add_argument('--input-csv', type=Path, default=Path('data/orginfo_companies.csv'))
    parser.add_argument('--db-path', type=Path, default=Path('data/makebiz.db'))
    parser.add_argument('--links-csv', type=Path, default=Path('data/orginfo_company_links.csv'))
    args = parser.parse_args()

    run_pipeline(args.input_csv, args.db_path, args.links_csv)
    print(f'Pipeline done. DB: {args.db_path}')
