from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable

DB_PATH = Path('data/makebiz.db')


def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('PRAGMA foreign_keys=ON;')
    return conn


def exec_many(conn: sqlite3.Connection, query: str, params: Iterable[tuple[Any, ...]]) -> None:
    conn.executemany(query, params)
    conn.commit()


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        '''
        CREATE TABLE IF NOT EXISTS source_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            rows_in INTEGER DEFAULT 0,
            rows_out INTEGER DEFAULT 0,
            notes TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url TEXT UNIQUE,
            company_name TEXT NOT NULL,
            company_name_raw TEXT DEFAULT '-',
            legal_form TEXT DEFAULT '-',
            short_name TEXT DEFAULT '-',
            inn TEXT DEFAULT '-',
            registration_date TEXT DEFAULT '-',
            activity_status TEXT DEFAULT '-',
            registration_authority TEXT DEFAULT '-',
            thsht TEXT DEFAULT '-',
            dbibt TEXT DEFAULT '-',
            ifut TEXT DEFAULT '-',
            charter_capital_uzs INTEGER DEFAULT 0,
            email TEXT DEFAULT '-',
            phone TEXT DEFAULT '-',
            address TEXT DEFAULT '-',
            region TEXT DEFAULT '-',
            district TEXT DEFAULT '-',
            category TEXT DEFAULT '-',
            tax_committee TEXT DEFAULT '-',
            large_taxpayer TEXT DEFAULT '-',
            director TEXT DEFAULT '-',
            founders TEXT DEFAULT '-',
            employees_count INTEGER DEFAULT 0,
            branch_count INTEGER DEFAULT 0,
            score INTEGER DEFAULT 0,
            score_label TEXT DEFAULT 'LOW',
            profile_completeness REAL DEFAULT 0,
            contact_valid INTEGER DEFAULT 0,
            score_explain TEXT DEFAULT '{}',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_companies_inn ON companies(inn);
        CREATE INDEX IF NOT EXISTS idx_companies_region ON companies(region);
        CREATE INDEX IF NOT EXISTS idx_companies_category ON companies(category);
        CREATE INDEX IF NOT EXISTS idx_companies_score ON companies(score);

        CREATE TABLE IF NOT EXISTS company_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            total_score INTEGER NOT NULL,
            completeness_score INTEGER DEFAULT 0,
            contacts_score INTEGER DEFAULT 0,
            activity_score INTEGER DEFAULT 0,
            profile_depth_score INTEGER DEFAULT 0,
            freshness_score INTEGER DEFAULT 0,
            explain_json TEXT DEFAULT '{}',
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_company_scores_company ON company_scores(company_id);

        CREATE TABLE IF NOT EXISTS dedup_conflicts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_a_id INTEGER NOT NULL,
            company_b_id INTEGER NOT NULL,
            reason TEXT NOT NULL,
            confidence REAL DEFAULT 0,
            status TEXT DEFAULT 'OPEN',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(company_a_id, company_b_id, reason),
            FOREIGN KEY(company_a_id) REFERENCES companies(id) ON DELETE CASCADE,
            FOREIGN KEY(company_b_id) REFERENCES companies(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS rfq (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            company_name TEXT NOT NULL,
            budget_uzs INTEGER DEFAULT 0,
            deadline TEXT DEFAULT '-',
            details TEXT DEFAULT '-',
            status TEXT DEFAULT 'OPEN',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS rfq_offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rfq_id INTEGER NOT NULL,
            company_id INTEGER NOT NULL,
            offer_amount_uzs INTEGER DEFAULT 0,
            status TEXT DEFAULT 'NEW',
            note TEXT DEFAULT '-',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(rfq_id) REFERENCES rfq(id) ON DELETE CASCADE,
            FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS company_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_company_id INTEGER NOT NULL,
            to_company_id INTEGER NOT NULL,
            relation_type TEXT NOT NULL,
            weight REAL DEFAULT 0,
            source TEXT DEFAULT 'system',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(from_company_id, to_company_id, relation_type),
            FOREIGN KEY(from_company_id) REFERENCES companies(id) ON DELETE CASCADE,
            FOREIGN KEY(to_company_id) REFERENCES companies(id) ON DELETE CASCADE
        );
        '''
    )
    conn.commit()

    # Lightweight forward migration for existing DBs.
    table_cols = {row[1] for row in conn.execute("PRAGMA table_info(companies)").fetchall()}
    if "company_name_raw" not in table_cols:
        conn.execute("ALTER TABLE companies ADD COLUMN company_name_raw TEXT DEFAULT '-'")
    if "legal_form" not in table_cols:
        conn.execute("ALTER TABLE companies ADD COLUMN legal_form TEXT DEFAULT '-'")
    conn.commit()
