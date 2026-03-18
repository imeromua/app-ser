"""
db.py — підключення до PostgreSQL та міграція схеми.

Використання:
    from db import get_conn, run_migrations
    run_migrations()   # один раз при старті
    with get_conn() as conn:
        conn.execute(...)
"""

import logging
import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

log = logging.getLogger(__name__)

# ── Пул підключень ────────────────────────────────────────────────────────────
_pool: ThreadedConnectionPool | None = None


def _get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        dsn = os.environ.get('DATABASE_URL')
        if not dsn:
            raise RuntimeError(
                'DATABASE_URL не задано. Додайте до .env: '
                'DATABASE_URL=postgresql://user:password@host:5432/dbname'
            )
        _pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=dsn,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        log.info('PostgreSQL connection pool created')
    return _pool


@contextmanager
def get_conn():
    """Контекстний менеджер: отримує з'єднання з пулу, повертає після use."""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# ── Міграція ──────────────────────────────────────────────────────────────────

_MIGRATION_SQL = """
-- Журнал імпортів
CREATE TABLE IF NOT EXISTS uploads (
    upload_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename      TEXT,
    shop          TEXT,
    warehouse     TEXT,
    period_from   TIMESTAMP,
    period_to     TIMESTAMP,
    uploaded_at   TIMESTAMP DEFAULT NOW(),
    strategy      VARCHAR(20),   -- FULL_INSERT / DELTA_INSERT / SKIP
    ops_inserted  INTEGER DEFAULT 0
);

-- Артикули (ціна оновлюється при кожному імпорті)
CREATE TABLE IF NOT EXISTS articles (
    article_id   VARCHAR(8) PRIMARY KEY,
    name         TEXT        NOT NULL,
    price        NUMERIC(10,2),
    updated_at   TIMESTAMP   DEFAULT NOW()
);

-- Операції — єдина накопичувальна таблиця
CREATE TABLE IF NOT EXISTS operations (
    id           BIGSERIAL   PRIMARY KEY,
    article_id   VARCHAR(8)  NOT NULL REFERENCES articles(article_id),
    doc_type     VARCHAR(10) NOT NULL,  -- ПрВ / Кнк / СпП / ПрИ / Апс
    doc_code     VARCHAR(60) NOT NULL,  -- X016-0337298
    subdoc_type  VARCHAR(10),           -- Ппт / СпО / Апк / ВИн / NULL
    subdoc_code  VARCHAR(60),           -- код піддокумента
    direction    VARCHAR(15),           -- до_нас / від_нас / NULL
    op_date      DATE        NOT NULL,
    qty          NUMERIC(10, 3) NOT NULL,
    col_source   CHAR(1)     NOT NULL,  -- G / H / I
    import_id    UUID        REFERENCES uploads(upload_id),
    UNIQUE (article_id, doc_code, op_date)  -- дедублікація при повторному імпорті
);

CREATE INDEX IF NOT EXISTS idx_operations_article   ON operations(article_id);
CREATE INDEX IF NOT EXISTS idx_operations_date      ON operations(op_date);
CREATE INDEX IF NOT EXISTS idx_operations_doc_type  ON operations(doc_type);

-- Знімки залишків з рядка '+' кожного імпорту (для валідації та порівняння)
CREATE TABLE IF NOT EXISTS article_snapshots (
    article_id    VARCHAR(8)     NOT NULL REFERENCES articles(article_id),
    snapshot_date DATE           NOT NULL,  -- = period_to імпорту
    upload_id     UUID           REFERENCES uploads(upload_id),
    balance_end   NUMERIC(10, 3),           -- J з рядка +
    total_in      NUMERIC(10, 3),           -- G з рядка +
    total_out     NUMERIC(10, 3),           -- H з рядка +
    price         NUMERIC(10, 2),           -- L з рядка +
    calc_balance  NUMERIC(10, 3),           -- розраховано парсером
    is_valid      BOOLEAN,                  -- calc_balance ~= balance_end
    PRIMARY KEY (article_id, snapshot_date)
);
"""


def run_migrations() -> None:
    """Виконує DDL міграцію (CREATE TABLE IF NOT EXISTS). Безпечно запускати повторно."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_MIGRATION_SQL)
    log.info('Database migrations applied')


# ── Утиліти ───────────────────────────────────────────────────────────────────

def get_max_op_date() -> 'datetime.date | None':
    """Повертає максимальну дату операції в БД (для дельта-імпорту)."""
    import datetime
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT MAX(op_date) AS max_date FROM operations')
            row = cur.fetchone()
            return row['max_date'] if row else None


def upsert_article(conn, article_id: str, name: str, price: float | None) -> None:
    """Додає або оновлює артикул (ціна і назва оновлюються завжди)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO articles (article_id, name, price, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (article_id) DO UPDATE
                SET name       = EXCLUDED.name,
                    price      = EXCLUDED.price,
                    updated_at = NOW()
            """,
            (article_id, name, price),
        )


def insert_operation(conn, *, article_id, doc_type, doc_code, subdoc_type,
                     subdoc_code, direction, op_date, qty, col_source,
                     import_id) -> bool:
    """
    Вставляє операцію. При дублікаті (UNIQUE) — пропускає.
    Повертає True якщо вставлено, False якщо дублікат.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO operations
                (article_id, doc_type, doc_code, subdoc_type, subdoc_code,
                 direction, op_date, qty, col_source, import_id)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (article_id, doc_code, op_date) DO NOTHING
            """,
            (article_id, doc_type, doc_code, subdoc_type, subdoc_code,
             direction, op_date, qty, col_source, import_id),
        )
        return cur.rowcount == 1


def upsert_snapshot(conn, *, article_id, snapshot_date, upload_id,
                    balance_end, total_in, total_out, price,
                    calc_balance) -> None:
    """Зберігає знімок залишків артикула для даного імпорту."""
    is_valid = abs((calc_balance or 0) - (balance_end or 0)) < 0.05
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO article_snapshots
                (article_id, snapshot_date, upload_id, balance_end,
                 total_in, total_out, price, calc_balance, is_valid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (article_id, snapshot_date) DO UPDATE
                SET balance_end  = EXCLUDED.balance_end,
                    total_in     = EXCLUDED.total_in,
                    total_out    = EXCLUDED.total_out,
                    price        = EXCLUDED.price,
                    calc_balance = EXCLUDED.calc_balance,
                    is_valid     = EXCLUDED.is_valid,
                    upload_id    = EXCLUDED.upload_id
            """,
            (article_id, snapshot_date, upload_id, balance_end,
             total_in, total_out, price, calc_balance, is_valid),
        )
