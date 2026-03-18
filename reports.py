"""
reports.py — SQL-функції для звітів по БД.

Всі функції використовують get_conn() з db.py та параметризовані запити.
Сума залишку = залишок × articles.price (остання актуальна ціна).
"""

import logging

from db import get_conn

log = logging.getLogger(__name__)


def get_summary_report(date_from, date_to) -> list[dict]:
    """Зведений звіт по всіх артикулах за період."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
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
                WHERE o.op_date BETWEEN %(date_from)s AND %(date_to)s
                GROUP BY a.article_id, a.name, a.price
                ORDER BY a.name
                """,
                {'date_from': date_from, 'date_to': date_to},
            )
            return [dict(row) for row in cur.fetchall()]


def get_inventory_report(date_from, date_to) -> list[dict]:
    """Звіт інвентаризацій (Апс) за період."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    a.article_id,
                    a.name,
                    a.price,
                    o.op_date        AS inv_date,
                    o.doc_code       AS inv_doc,
                    o.subdoc_code    AS vin_doc,
                    o.qty            AS inv_qty,
                    o.qty * a.price  AS inv_sum
                FROM operations o
                JOIN articles a USING (article_id)
                WHERE o.doc_type = 'Апс'
                  AND o.op_date BETWEEN %(date_from)s AND %(date_to)s
                ORDER BY o.op_date, a.name
                """,
                {'date_from': date_from, 'date_to': date_to},
            )
            return [dict(row) for row in cur.fetchall()]


def get_top_sales(date_from, date_to, limit: int = 15) -> list[dict]:
    """Топ-N артикулів за продажами (Кнк) за період."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    a.article_id, a.name, a.price,
                    SUM(ABS(o.qty))           AS total_sold,
                    SUM(ABS(o.qty)) * a.price AS total_sum
                FROM operations o
                JOIN articles a USING (article_id)
                WHERE o.doc_type = 'Кнк'
                  AND o.op_date BETWEEN %(date_from)s AND %(date_to)s
                GROUP BY a.article_id, a.name, a.price
                ORDER BY total_sold DESC
                LIMIT %(limit)s
                """,
                {'date_from': date_from, 'date_to': date_to, 'limit': limit},
            )
            return [dict(row) for row in cur.fetchall()]


def get_zero_balance(date_from, date_to) -> list[dict]:
    """Артикули з нульовим або від'ємним залишком за період."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    a.article_id, a.name, a.price,
                    SUM(o.qty) AS balance
                FROM operations o
                JOIN articles a USING (article_id)
                WHERE o.op_date BETWEEN %(date_from)s AND %(date_to)s
                GROUP BY a.article_id, a.name, a.price
                HAVING SUM(o.qty) <= 0
                ORDER BY a.name
                """,
                {'date_from': date_from, 'date_to': date_to},
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
