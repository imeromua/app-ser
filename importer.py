"""
importer.py — імпорт даних з XLS-звіту «Рух товарів» до PostgreSQL.

Використання:
    from importer import run_import
    result = run_import(buf, 'ruh_tovariv_2026.xls')
"""

import logging
import uuid
from datetime import date, timedelta

from db import (
    get_conn,
    get_max_op_date,
    insert_operation,
    upsert_article,
    upsert_snapshot,
)
from parser import parse_xls
from reports import get_balance_discrepancies

log = logging.getLogger(__name__)


def decide_strategy(new_period_to: date, db_max_date) -> str:
    """
    Визначає стратегію імпорту:
      FULL_INSERT   — БД порожня (перший імпорт)
      DELTA_INSERT  — new_period_to > db_max_date (є нові дані)
      SKIP          — new_period_to <= db_max_date (всі дані вже є)
    """
    if db_max_date is None:
        return 'FULL_INSERT'
    if new_period_to > db_max_date:
        return 'DELTA_INSERT'
    return 'SKIP'


def validate_snapshots(conn, upload_id: str) -> list:
    """
    Повертає список артикулів з розбіжністю між розрахованим (calc_balance)
    і контрольним (balance_end) залишком більше 0.05.
    Логує WARNING для кожного такого артикула.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT article_id, balance_end, calc_balance
            FROM article_snapshots
            WHERE upload_id = %s AND is_valid = FALSE
            """,
            (upload_id,),
        )
        rows = cur.fetchall()

    invalid = []
    for row in rows:
        log.warning(
            'Snapshot mismatch — article=%s: balance_end=%s, calc_balance=%s',
            row['article_id'], row['balance_end'], row['calc_balance'],
        )
        invalid.append(dict(row))
    return invalid


def run_import(buf, filename: str) -> dict:
    """
    Головна функція імпорту XLS-файлу до PostgreSQL.

    Кроки:
      1. parse_xls(buf) → header + articles + operations
      2. decide_strategy() → FULL_INSERT / DELTA_INSERT / SKIP
      3. SKIP → повернути інфо без запису в БД
      4. DELTA_INSERT → вставляти операції де op_date > (db_max_date − 30 днів)
      5. upsert_article + upsert_snapshot + insert_operation (ON CONFLICT DO NOTHING)
      6. validate_snapshots() → порівняти calc_balance vs balance_end
      7. Повернути статистику

    Повертає dict:
      {'strategy', 'upload_id', 'ops_inserted', 'articles_count', 'invalid_snapshots'}
    або при SKIP:
      {'strategy': 'SKIP', 'period_to', 'db_max_date'}
    """
    result     = parse_xls(buf)
    header     = result['header']
    articles   = result['articles']
    operations = result['operations']

    period_to = header.get('period_to')
    if period_to is None:
        raise ValueError('Не вдалося визначити кінець звітного періоду з файлу')

    db_max_date = get_max_op_date()
    strategy    = decide_strategy(period_to, db_max_date)

    if strategy == 'SKIP':
        log.info('Import SKIP: period_to=%s <= db_max_date=%s', period_to, db_max_date)
        return {
            'strategy':    'SKIP',
            'period_to':   str(period_to),
            'db_max_date': str(db_max_date),
        }

    # Для DELTA_INSERT вставляємо тільки операції з перекриттям 30 днів.
    # 30 днів обрано для покриття можливих ретроактивних коригувань Апс
    # (інвентаризація може бути датована кількома тижнями раніше).
    ops_to_insert = operations
    if strategy == 'DELTA_INSERT':
        cutoff = db_max_date - timedelta(days=30)
        ops_to_insert = [
            op for op in operations
            if op['op_date'] and op['op_date'] > cutoff
        ]
        log.info(
            'DELTA_INSERT: cutoff=%s, filtered %d → %d ops',
            cutoff, len(operations), len(ops_to_insert),
        )

    # calc_balance розраховується з ПОВНОГО списку операцій (не відфільтрованого)
    article_ops: dict = {}
    for op in operations:
        article_ops.setdefault(op['article_id'], []).append(op)

    upload_id    = str(uuid.uuid4())
    ops_inserted = 0

    with get_conn() as conn:
        # Запис про імпорт
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO uploads
                    (upload_id, filename, shop, warehouse, period_from, period_to, strategy)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    upload_id,
                    filename,
                    header.get('shop'),
                    header.get('warehouse'),
                    header.get('period_from'),
                    header.get('period_to'),
                    strategy,
                ),
            )

        snapshot_date = period_to

        for art in articles:
            article_id = art['article_id']
            upsert_article(conn, article_id, art['name'], art.get('price'),
                           last_seen_date=period_to,
                           balance_control=art.get('balance_end'))

            ops_for_art  = article_ops.get(article_id, [])
            calc_balance = round(sum(op['qty'] for op in ops_for_art), 3)

            upsert_snapshot(
                conn,
                article_id=article_id,
                snapshot_date=snapshot_date,
                upload_id=upload_id,
                balance_end=art.get('balance_end'),
                total_in=art.get('total_in'),
                total_out=art.get('total_out'),
                price=art.get('price'),
                calc_balance=calc_balance,
            )

        for op in ops_to_insert:
            inserted = insert_operation(
                conn,
                article_id=op['article_id'],
                doc_type=op['doc_type'],
                doc_code=op['doc_code'],
                subdoc_type=op.get('subdoc_type'),
                subdoc_code=op.get('subdoc_code'),
                direction=op.get('direction'),
                op_date=op['op_date'],
                qty=op['qty'],
                col_source=op['col_source'],
                import_id=upload_id,
            )
            if inserted:
                ops_inserted += 1

        # Оновлюємо лічильник вставлених операцій
        with conn.cursor() as cur:
            cur.execute(
                'UPDATE uploads SET ops_inserted = %s WHERE upload_id = %s',
                (ops_inserted, upload_id),
            )

        invalid_snapshots = validate_snapshots(conn, upload_id)

    # Перевірка розбіжностей залишків після імпорту
    try:
        discrepancies = get_balance_discrepancies()
        for row in discrepancies:
            log.warning(
                'Balance discrepancy — article=%s (%s): expected=%s, calculated=%s, diff=%s',
                row['article_id'], row['name'],
                row['expected'], row['calculated'], row['diff'],
            )
        if discrepancies:
            log.warning(
                'Import %s: %d article(s) with balance discrepancies after import',
                strategy, len(discrepancies),
            )
    except Exception:
        log.exception('Failed to check balance discrepancies after import')

    log.info(
        'Import %s done: articles=%d, ops_inserted=%d, invalid_snapshots=%d',
        strategy, len(articles), ops_inserted, len(invalid_snapshots),
    )

    return {
        'strategy':          strategy,
        'upload_id':         upload_id,
        'ops_inserted':      ops_inserted,
        'articles_count':    len(articles),
        'invalid_snapshots': invalid_snapshots,
    }
