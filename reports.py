"""
reports.py — SQL-функції для звітів по БД.

Всі функції використовують get_conn() з db.py та параметризовані запити.
Сума залишку = залишок × articles.price (остання актуальна ціна).
"""

import logging

from db import get_conn

log = logging.getLogger(__name__)


def _build_keyword_filter(keywords: 'list[str] | None') -> tuple[str, list]:
    """Повертає SQL-умову та параметри для фільтрації по ключових словах назви артикула."""
    if not keywords:
        return '', []
    conditions = ' OR '.join('a.name ILIKE %s' for _ in keywords)
    params = [f'%{kw}%' for kw in keywords]
    return f' AND ({conditions})', params


def get_summary_report(date_from, date_to, keywords: 'list[str] | None' = None) -> list[dict]:
    """Зведений звіт по всіх артикулах за період."""
    kw_sql, kw_params = _build_keyword_filter(keywords)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    a.article_id,
                    a.name,
                    a.price,
                    SUM(CASE WHEN o.doc_type = 'ПрВ' THEN o.qty ELSE 0 END)       AS total_in,
                    SUM(CASE WHEN o.doc_type = 'Кнк' THEN ABS(o.qty) ELSE 0 END)  AS total_sales,
                    SUM(CASE WHEN o.doc_type = 'СпП' THEN ABS(o.qty) ELSE 0 END)  AS total_writeoff,
                    SUM(CASE WHEN o.doc_type = 'ПрИ' THEN ABS(o.qty) ELSE 0 END)  AS total_transfer,
                    SUM(CASE WHEN o.doc_type = 'Апс' THEN o.qty ELSE 0 END)        AS total_inv,
                    SUM(o.qty)                                                      AS balance,
                    SUM(o.qty) * a.price                                            AS balance_sum
                FROM operations o
                JOIN articles a USING (article_id)
                WHERE o.op_date BETWEEN %s AND %s{kw_sql}
                GROUP BY a.article_id, a.name, a.price
                ORDER BY a.name
                """,
                [date_from, date_to] + kw_params,
            )
            return [dict(row) for row in cur.fetchall()]


def get_inventory_report(date_from, date_to, keywords: 'list[str] | None' = None) -> list[dict]:
    """Звіт інвентаризацій (Апс) за період."""
    kw_sql, kw_params = _build_keyword_filter(keywords)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    a.article_id,
                    a.name,
                    a.price,
                    o.op_date        AS inv_date,
                    o.doc_type || '/' || o.doc_code AS inv_doc,
                    o.subdoc_code    AS vin_doc,
                    o.qty            AS inv_qty,
                    o.qty * a.price  AS inv_sum
                FROM operations o
                JOIN articles a USING (article_id)
                WHERE o.doc_type = 'Апс'
                  AND o.op_date BETWEEN %s AND %s{kw_sql}
                ORDER BY o.op_date, a.name
                """,
                [date_from, date_to] + kw_params,
            )
            return [dict(row) for row in cur.fetchall()]


def get_top_sales(date_from, date_to, limit: int = 15, keywords: 'list[str] | None' = None) -> list[dict]:
    """Топ-N артикулів за продажами (Кнк) за період."""
    kw_sql, kw_params = _build_keyword_filter(keywords)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    a.article_id, a.name, a.price,
                    SUM(ABS(o.qty))           AS total_sold,
                    SUM(ABS(o.qty)) * a.price AS total_sum
                FROM operations o
                JOIN articles a USING (article_id)
                WHERE o.doc_type = 'Кнк'
                  AND o.op_date BETWEEN %s AND %s{kw_sql}
                GROUP BY a.article_id, a.name, a.price
                ORDER BY total_sold DESC
                LIMIT %s
                """,
                [date_from, date_to] + kw_params + [limit],
            )
            return [dict(row) for row in cur.fetchall()]


def get_zero_balance(date_from, date_to, keywords: 'list[str] | None' = None) -> list[dict]:
    """Артикули з нульовим або від'ємним залишком за період."""
    kw_sql, kw_params = _build_keyword_filter(keywords)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    a.article_id, a.name, a.price,
                    SUM(o.qty) AS balance
                FROM operations o
                JOIN articles a USING (article_id)
                WHERE o.op_date BETWEEN %s AND %s{kw_sql}
                GROUP BY a.article_id, a.name, a.price
                HAVING SUM(o.qty) <= 0
                ORDER BY a.name
                """,
                [date_from, date_to] + kw_params,
            )
            return [dict(row) for row in cur.fetchall()]


def get_missing_articles(days: int = 30) -> list[dict]:
    """Артикули, відсутні у файлах більше N днів."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT article_id, name, price, last_seen_date
                FROM articles
                WHERE last_seen_date < CURRENT_DATE - INTERVAL '1 day' * %(days)s
                   OR last_seen_date IS NULL
                ORDER BY last_seen_date
                """,
                {'days': days},
            )
            return [dict(row) for row in cur.fetchall()]


def get_balance_discrepancies() -> list[dict]:
    """
    Повертає артикули, де SUM(qty) з operations відрізняється від balance_control (J з XLS).

    Використовується після імпорту для перевірки цілісності даних.
    Допустима похибка: 0.05 (аналогічно validate_snapshots).
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    a.article_id,
                    a.name,
                    a.balance_control               AS expected,
                    SUM(o.qty)                      AS calculated,
                    SUM(o.qty) - a.balance_control  AS diff
                FROM operations o
                JOIN articles a USING (article_id)
                WHERE a.balance_control IS NOT NULL
                GROUP BY a.article_id, a.name, a.balance_control
                HAVING ABS(SUM(o.qty) - a.balance_control) > 0.05
                ORDER BY ABS(SUM(o.qty) - a.balance_control) DESC
                """
            )
            return [dict(row) for row in cur.fetchall()]


def get_inventory_template() -> list[dict]:
    """
    Поточний залишок по всіх артикулах для відомості інвентаризації.

    Повертає: [{'Артикул': ..., 'Назва': ..., 'Залишок': ...}, ...]
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    a.article_id  AS "Артикул",
                    a.name        AS "Назва",
                    SUM(o.qty)    AS "Залишок"
                FROM operations o
                JOIN articles a USING (article_id)
                GROUP BY a.article_id, a.name
                HAVING SUM(o.qty) > 0
                ORDER BY a.name
                """
            )
            return [dict(row) for row in cur.fetchall()]
