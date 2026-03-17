"""
parser.py — парсинг XLS-файлів звіту EPI.

Підтримує .xls та .xlsx, стійкий до зсуву колонок.
Динамічно визначає колонки Прихід/Розхід/Інв за шапкою.
"""

import logging
import re

import pandas as pd

# Типи документів, що розпізнаються (підрядкове входження)
_DOC_KEYWORDS = ('Кнк', 'СпО', 'СпП', 'ПрВ', 'ПрИ', 'Апк', 'Апс', 'Ппт')

# X016 — код магазину для Ппт. Перевіряємо обидва варіанти:
# латинська ASCII 'X' (0x58) та кирилична 'Х' (U+0425) — виглядають однаково!
_SHOP_CODE      = 'X016'   # Latin X
_SHOP_CODE_CYR  = '\u0425016'  # Cyrillic Х

_MAX_HEADER_SEARCH_ROWS = 50

ALLOWED_MIME_HEADERS = (
    b'\xd0\xcf\x11\xe0',  # .xls  (OLE2)
    b'PK\x03\x04',        # .xlsx (ZIP/OOXML)
)


def is_article_code(val):
    s = str(val).strip()
    return s.isdigit() and len(s) >= 5


def _is_own_shop(doc_text: str) -> bool:
    """
    Повертає True якщо Ппт-документ стосується власного магазину (= РОЗХІД).
    Ппт/X016... або Ппт/Х016... — це РОЗХІД (переміщення З магазину).
    Ппт/<інший код> — це ПРИХІД (переміщення В магазин).
    Перевіряємо обидва варіанти X: ASCII Latin та Unicode Cyrillic.
    """
    m = re.search(r'[\u041f\u043f][\u041f\u043f][\u0422\u0442][/\\](\S+)', doc_text)
    if not m:
        t = doc_text.upper()
        return (_SHOP_CODE.upper() in t) or (_SHOP_CODE_CYR.upper() in t)
    after_slash = m.group(1).upper()
    # Нормалізація: кирилична Х → латинська X
    after_slash_norm = after_slash.replace('\u0425', 'X')
    shop_norm = _SHOP_CODE.upper()
    return after_slash_norm.startswith(shop_norm)


def _classify_operation(doc_text: str):
    """
    Розпізнає тип операції за підрядком у назві документа.
    Порядок перевірки важливий: перший збіг виграє.
    Повертає рядок назви операції, що ТОЧНО збігається з ключами _OP_TO_COL у builder.py.
    """
    if 'Кнк' in doc_text:
        return 'Кнк (Продаж)'
    if 'СпО' in doc_text or 'СпП' in doc_text:
        return 'СпП (Списання)'
    if 'ПрВ' in doc_text:
        return 'ПрВ (Прихід)'
    if 'ПрИ' in doc_text:
        return 'ПрИ (Переміщення)'
    if 'Апк' in doc_text or 'Апс' in doc_text:
        return 'Апк (Корегування)'
    if re.search(r'[\u041f\u043f][\u041f\u043f][\u0422\u0442]', doc_text):
        # Ппт/X016 або Ппт/Х016 — товар ІДЕ з нашого магазину (розхід)
        # Ппт/<інший> — товар ПРИХОДИТЬ до нас (прихід)
        if _is_own_shop(doc_text):
            return 'Ппт (Переміщення Розхід)'
        return 'Ппт (Переміщення Прихід)'
    return None


def _find_financial_cols(df):
    """
    Шукає рядок шапки з 'Прихід' та 'Розхід'.
    Повертає (header_row_idx, pryhid_col, rozkhid_col, inv_col).
    """
    for i in range(0, min(_MAX_HEADER_SEARCH_ROWS, len(df))):
        row_str = ' '.join(str(v).strip() for v in df.iloc[i] if pd.notna(v))
        if 'Прихід' in row_str and 'Розхід' in row_str:
            pryhid_col = rozkhid_col = inv_col = None
            for j, v in enumerate(df.iloc[i]):
                s = str(v).strip() if pd.notna(v) else ''
                if 'Прихід' in s and pryhid_col is None:
                    pryhid_col = j
                elif 'Розхід' in s and rozkhid_col is None:
                    rozkhid_col = j
                elif 'Інв' in s and inv_col is None:
                    inv_col = j
            if pryhid_col is not None and rozkhid_col is not None:
                return i, pryhid_col, rozkhid_col, inv_col
    return None, None, None, None


def find_data_start(df, after_row=0):
    """Знаходить перший рядок з маркером '+' та артикулом."""
    for i in range(max(after_row, 5), min(after_row + _MAX_HEADER_SEARCH_ROWS + 10, len(df))):
        try:
            if df.iloc[i, 0] == '+' and is_article_code(str(df.iloc[i, 1]).strip()):
                return i
        except Exception:
            continue
    return after_row + 15


def parse_xls(buf):
    # ── MIME-type validation ──────────────────────────────────────────────
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

    # ── Шапка звіту ──────────────────────────────────────────────────────
    header = {
        'title':     cell(0, 0),
        'shop':      cell(1, 3),
        'period':    cell(1, 10),
        'warehouse': cell(3, 3),
    }
    # Додатковий пошук Магазин:/Склад: якщо стандартні позиції порожні
    if not header['shop'] or not header['warehouse']:
        for r in range(min(20, len(df))):
            for c in range(len(df.columns)):
                val = str(df.iloc[r, c]).strip()
                if not val or val == 'nan':
                    continue
                if 'Магазин:' in val and not header['shop']:
                    if len(val) > 10:
                        header['shop'] = val.replace('Магазин:', '').strip()
                    else:
                        for nc in range(c + 1, len(df.columns)):
                            nval = str(df.iloc[r, nc]).strip()
                            if nval and nval not in ('nan', '-'):
                                header['shop'] = nval
                                break
                elif 'Склад:' in val and not header['warehouse']:
                    if len(val) > 8:
                        header['warehouse'] = val.replace('Склад:', '').strip()
                    else:
                        for nc in range(c + 1, len(df.columns)):
                            nval = str(df.iloc[r, nc]).strip()
                            if nval and nval not in ('nan', '-'):
                                header['warehouse'] = nval
                                break
                if re.search(r'\d{2}\.\d{2}\.\d{2,4}\s+\d{1,2}:\d{2}', val):
                    header['period'] = val

    # ── Динамічний пошук фінансових колонок ──────────────────────────────
    hdr_row, pryhid_col, rozkhid_col, inv_col = _find_financial_cols(df)
    data_start = find_data_start(df, after_row=(hdr_row or 0))

    def _get_float(row, col_idx):
        if col_idx is None:
            return 0.0
        try:
            v = row.iloc[col_idx]
            return float(v) if pd.notna(v) else 0.0
        except Exception:
            return 0.0

    data = []
    i = data_start
    cur_art, cur_name = None, ''
    last_date = None

    while i < len(df):
        row    = df.iloc[i]
        marker = row.iloc[0] if len(row) > 0 else None
        col1   = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''

        if marker == '+' and is_article_code(col1):
            cur_art  = col1
            cur_name = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ''
            i += 1
            continue

        if marker == '-':
            if is_article_code(col1):
                cur_art  = col1
                cur_name = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else cur_name
                i += 1
                continue

            op = _classify_operation(col1) if cur_art else None
            if op:
                pryhid_val  = _get_float(row, pryhid_col)
                rozkhid_val = _get_float(row, rozkhid_col)
                inv_val     = _get_float(row, inv_col)
                qty = pryhid_val - rozkhid_val + inv_val

                if qty == 0 and pryhid_val == 0 and rozkhid_val == 0 and inv_val == 0:
                    i += 1
                    continue

                m = re.search(r'(\d{2}\.\d{2}\.\d{2})', col1)
                if m:
                    try:
                        last_date = pd.to_datetime(m.group(1), format='%d.%m.%y')
                    except Exception as e:
                        logging.warning(f"Рядок {i}: не вдалося розпізнати дату '{m.group(1)}': {e}")
                d  = last_date
                ym = f"{d.year}-{d.month:02d}" if d is not None else ''

                data.append({
                    'Артикул':    cur_art,
                    'Назва':      cur_name,
                    'Операція':   op,
                    'Документ':   col1,
                    'Дата':       d,
                    'Рік-Місяць': ym,
                    'Кількість':  qty,
                    'Прихід':     pryhid_val,
                    'Розхід':     rozkhid_val,
                })
        i += 1

    # ── Ціни (з '+'-рядків кожного артикулу) ─────────────────────────────
    prices = {}
    if inv_col is not None:
        price_col = inv_col + 3
    elif rozkhid_col is not None:
        price_col = rozkhid_col + 4
    else:
        price_col = 11
    for i2 in range(data_start, len(df)):
        r2 = df.iloc[i2]
        if r2.iloc[0] == '+' and is_article_code(str(r2.iloc[1]).strip()):
            try:
                p = float(r2.iloc[price_col])
                if p > 0:
                    prices[str(r2.iloc[1]).strip()] = p
            except Exception:
                try:
                    p = float(r2.iloc[11])
                    if p > 0:
                        prices[str(r2.iloc[1]).strip()] = p
                except Exception:
                    pass

    return header, pd.DataFrame(data), prices
