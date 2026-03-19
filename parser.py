"""
parser.py — парсинг XLS-файлів звіту «Рух товарів» EPI.

Підтримує .xls та .xlsx.
"""

import logging
import re
from datetime import date, datetime

import pandas as pd

log = logging.getLogger(__name__)

ALLOWED_MIME_HEADERS = (
    b'\xd0\xcf\x11\xe0',  # .xls  (OLE2)
    b'PK\x03\x04',        # .xlsx (ZIP/OOXML)
)


def is_article_code(val) -> bool:
    """Повертає True для рядка з рівно 8 цифрами (код артикула)."""
    s = str(val).strip()
    return s.isdigit() and len(s) == 8


def _parse_period(period_str: str) -> tuple:
    """
    Розбирає рядок "01.01.24   0:00 - 18.03.26  23:59" у (period_from, period_to).
    Повертає tuple[date | None, date | None].
    """
    parts = re.findall(r'(\d{2}\.\d{2}\.\d{2,4})', period_str)

    def to_date(s):
        for fmt in ('%d.%m.%y', '%d.%m.%Y'):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    period_from = to_date(parts[0]) if len(parts) > 0 else None
    period_to   = to_date(parts[1]) if len(parts) > 1 else None
    return period_from, period_to


def _has_date(text: str) -> bool:
    """Перевіряє наявність дати у форматі DD.MM.YY в тексті."""
    return bool(re.search(r'\d{2}\.\d{2}\.\d{2}\b', text))


def _is_subdoc(text: str) -> bool:
    """Піддокумент — не містить дати, але має '/' між кодами."""
    return not _has_date(text) and '/' in text


def _extract_date(text: str):
    """Витягує дату DD.MM.YY з тексту. Повертає date або None."""
    m = re.search(r'(\d{2}\.\d{2}\.\d{2})\b', text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), '%d.%m.%y').date()
    except ValueError:
        return None


def _extract_doc_type_code(text: str) -> tuple:
    """
    Витягує (doc_type, doc_code) з рядка основного документа.
    Приклад: 'ПрВ/X016-0337298 - 13.11.25' → ('ПрВ', 'X016-0337298')
    """
    m = re.match(r'^(.{3})/(\S+)', text.strip())
    if m:
        return m.group(1), m.group(2)
    return (text[:3] if len(text) >= 3 else text), ''


def classify_subdoc(subdoc_text: str) -> tuple:
    """
    Визначає тип піддокумента.
    Повертає (subdoc_type, subdoc_code, direction).
    direction: 'до_нас' / 'від_нас' / None
    """
    if re.search(r'Ппт/[X\u0425]016', subdoc_text):
        code = re.search(r'Ппт/([X\u0425]\w*-\d+)', subdoc_text)
        return 'Ппт', code.group(1) if code else '', 'від_нас'

    if re.search(r'Ппт/(FDL|DP)', subdoc_text):
        code = re.search(r'Ппт/(\w+-\d+)', subdoc_text)
        return 'Ппт', code.group(1) if code else '', 'до_нас'

    if 'СпО' in subdoc_text:
        code = re.search(r'СпО/(\w+-\d+)', subdoc_text)
        return 'СпО', code.group(1) if code else '', None

    if 'Апк' in subdoc_text:
        code = re.search(r'Апк/(\w+-\d+)', subdoc_text)
        return 'Апк', code.group(1) if code else '', None

    if 'ВИн' in subdoc_text:
        code = re.search(r'ВИн/(\w+-\d+)', subdoc_text)
        return 'ВИн', code.group(1) if code else '', None

    if 'Зпт' in subdoc_text:
        return 'Зпт', '', None

    return None, None, None


def get_qty(row, doc_type: str) -> tuple:
    """
    Повертає (qty, col_source) де col_source = 'G' / 'H' / 'I'.
    ПрВ/СпП/ПрИ → col G (index 6); знак береться з файлу
    Кнк          → col H (index 7), інвертується (продаж = мінус до залишку)
    Воз          → col H (index 7), береться як є (EPI пише від'ємне число,
                   тобто повернення = плюс до залишку)
    Апс          → col I (index 8), позитивна кількість одиниць до списання;
                   завжди зменшує залишок — інвертується
    """
    def safe_float(idx):
        try:
            v = row.iloc[idx]
            return float(v) if pd.notna(v) else 0.0
        except Exception:
            return 0.0

    if doc_type == 'Кнк':
        return -safe_float(7), 'H'
    if doc_type == 'Воз':
        # EPI записує Воз як від'ємне значення в col H → без інверсії = +qty
        return safe_float(7), 'H'
    if doc_type == 'Апс':
        return -safe_float(8), 'I'
    return safe_float(6), 'G'


def _is_op_marker(marker) -> bool:
    """
    Повертає True якщо маркер вказує на рядок операції.
    EPI використовує '-' для звичайних операцій,
    '- ч' (або будь-який рядок що починається з '-') для Апс-корегувань.
    """
    if marker is None:
        return False
    s = str(marker).strip()
    return s == '-' or s.startswith('-')


def _get_balance_start(row) -> float:
    """
    Читає початковий залишок (залишок на початок періоду) з рядка артикула.
    EPI: col E (index 4) — "Залишок на початок".
    Повертає float (0.0 якщо порожньо або відсутнє).
    """
    try:
        v = row.iloc[4]
        return float(v) if pd.notna(v) else 0.0
    except Exception:
        return 0.0


def parse_xls(buf) -> dict:
    """
    Парсить XLS/XLSX файл звіту «Рух товарів».

    Повертає:
    {
        'header': {'shop', 'warehouse', 'period_from', 'period_to', 'title', 'period'},
        'articles': [{'article_id', 'name', 'price', 'total_in', 'total_out', 'balance_end'}],
        'operations': [{'article_id', 'doc_type', 'doc_code', 'subdoc_type',
                        'subdoc_code', 'direction', 'op_date', 'qty', 'col_source'}]
    }
    """
    buf.seek(0)
    header_bytes = buf.read(4)
    if not any(header_bytes.startswith(sig) for sig in ALLOWED_MIME_HEADERS):
        raise ValueError('Невірний тип файлу: дозволено лише .xls та .xlsx')
    buf.seek(0)
    df = pd.read_excel(buf, sheet_name=0, header=None)

    def cell(r, c):
        try:
            v = df.iloc[r, c]
            return str(v).strip() if pd.notna(v) else ''
        except Exception:
            return ''

    def safe_float_at(row, idx):
        try:
            v = row.iloc[idx]
            return float(v) if pd.notna(v) else None
        except Exception:
            return None

    title      = cell(0, 0)
    shop       = cell(1, 3)
    period_str = cell(1, 10)
    warehouse  = cell(3, 3)

    period_from, period_to = _parse_period(period_str) if period_str else (None, None)

    header = {
        'title':       title,
        'shop':        shop,
        'warehouse':   warehouse,
        'period':      period_str,
        'period_from': period_from,
        'period_to':   period_to,
    }

    articles: list = []
    operations: list = []
    cur_article = None
    pending_op = None
    running_balances: dict = {}

    for i in range(len(df)):
        row    = df.iloc[i]
        marker = row.iloc[0] if len(row) > 0 else None
        col_b  = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''

        if not col_b and not _is_op_marker(marker):
            continue

        # ── Рядок артикула ──────────────────────────────────────────────────────────
        if is_article_code(col_b):
            col_c = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ''
            balance_start = _get_balance_start(row)
            cur_article = {
                'article_id':    col_b,
                'name':          col_c,
                'price':         safe_float_at(row, 11),
                'total_in':      safe_float_at(row, 6),
                'total_out':     safe_float_at(row, 7),
                'balance_start': balance_start,
                'balance_end':   safe_float_at(row, 9),
            }
            articles.append(cur_article)
            running_balances[col_b] = balance_start
            pending_op = None
            continue

        # ── Рядки операцій ('-' або '- ч' тощо) ─────────────────────────────
        if not _is_op_marker(marker) or cur_article is None:
            continue

        if not col_b:
            continue

        if _has_date(col_b):
            pending_op = None
            doc_type, doc_code = _extract_doc_type_code(col_b)
            op_date = _extract_date(col_b)
            qty, col_src = get_qty(row, doc_type)
            article_id = cur_article['article_id']

            if qty == 0:
                continue

            running_balances[article_id] = round(
                running_balances.get(article_id, 0.0) + qty, 3
            )

            op = {
                'article_id':  article_id,
                'doc_type':    doc_type,
                'doc_code':    doc_code,
                'subdoc_type': None,
                'subdoc_code': None,
                'direction':   None,
                'op_date':     op_date,
                'qty':         qty,
                'col_source':  col_src,
            }
            operations.append(op)
            pending_op = op

        elif _is_subdoc(col_b):
            subdoc_type, subdoc_code, direction = classify_subdoc(col_b)
            if pending_op is not None:
                pending_op['subdoc_type'] = subdoc_type
                pending_op['subdoc_code'] = subdoc_code
                pending_op['direction']   = direction
            pending_op = None

    return {
        'header':     header,
        'articles':   articles,
        'operations': operations,
    }


# ── Допоміжня функція для зворотної сумісності з app.py ─────────────────────────────

def op_display_name(doc_type: str, subdoc_type, direction) -> str:
    """
    Перетворює нові поля (doc_type, subdoc_type, direction) на назву операції
    для builder.py (сумісність з ключами _OP_TO_COL).
    """
    if doc_type == 'ПрВ':
        return 'ПрВ (Прихід)'
    if doc_type == 'Кнк':
        return 'Кнк (Продаж)'
    if doc_type == 'Воз':
        return 'Воз (Повернення)'
    if doc_type == 'СпП':
        return 'СпП (Списання)'
    if doc_type == 'ПрИ':
        return 'ПрИ (Переміщення)'
    if doc_type == 'Апс':
        if subdoc_type == 'Ппт' and direction == 'від_нас':
            return 'Ппт (Переміщення Розхід)'
        if subdoc_type == 'Ппт' and direction == 'до_нас':
            return 'Ппт (Переміщення Прихід)'
        return 'Апк (Корегування)'
    return 'Апк (Корегування)'
