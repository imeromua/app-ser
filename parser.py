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

ALLOWED_MIME_HEADERS = (
    b'\xd0\xcf\x11\xe0',  # .xls  (OLE2)
    b'PK\x03\x04',        # .xlsx (ZIP/OOXML)
)


def is_article_code(val):
    s = str(val).strip()
    return s.isdigit() and len(s) >= 5


def _classify_operation(doc_text: str):
    """
    Розпізнає тип операції за підрядком у назві документа.
    Порядок перевірки важливий: перший збіг виграє.
    Повертає (canonical_op, display_op) або None якщо не розпізнано.
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
    if 'Ппт' in doc_text:
        if 'Ппт/X016' in doc_text:
            return 'Ппт (Переміщення Розхід)'
        return 'Ппт (Переміщення Прихід)'
    return None


def _find_financial_cols(df):
    """
    Шукає рядок з шапкою, де є 'Прихід' та 'Розхід'.
    Повертає (header_row_idx, pryhid_col, rozkhid_col, inv_col).
    Якщо шапка не знайдена — повертає (None, None, None, None).
    """
    for i in range(0, min(50, len(df))):
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
    """Знаходить перший рядок з маркером '+' та артикулом після after_row."""
    for i in range(max(after_row, 5), min(after_row + 60, len(df))):
        try:
            if df.iloc[i, 0] == '+' and is_article_code(str(df.iloc[i, 1]).strip()):
                return i
        except Exception:
            continue
    return after_row + 15  # fallback


def parse_xls(buf):
    # ── MIME-type validation via magic bytes ──────────────────────────────
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

    header = {
        'title':     cell(0, 0),
        'shop':      cell(1, 3),
        'period':    cell(1, 10),
        'warehouse': cell(3, 3),
    }

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

                # Рядок включаємо навіть без дати, якщо є фінансовий рух
                if qty == 0 and pryhid_val == 0 and rozkhid_val == 0 and inv_val == 0:
                    i += 1
                    continue

                m = re.search(r'(\d{2}\.\d{2}\.\d{2})', col1)
                if m:
                    try:
                        last_date = pd.to_datetime(m.group(1), format='%d.%m.%y')
                    except Exception as e:
                        logging.warning(f"Рядок {i}: не вдалося розпізнати дату '{m.group(1)}': {e}")
                d = last_date
                ym = f"{d.year}-{d.month:02d}" if d is not None else ''

                data.append({
                    'Артикул':   cur_art,
                    'Назва':     cur_name,
                    'Операція':  op,
                    'Документ':  col1,
                    'Дата':      d,
                    'Рік-Місяць': ym,
                    'Кількість': qty,
                    'Прихід':    pryhid_val,
                    'Розхід':    rozkhid_val,
                })
        i += 1

    # ── Ціни (перший '+'-рядок кожного артикулу) ─────────────────────────
    # Типова структура: прихід(K-ть) | розхід(K-ть) | інв | прихід(сума) | розхід(сума) | ціна
    # Тобто ціна зазвичай на 3 позиції правіше від Інв колонки.
    prices = {}
    if inv_col is not None:
        price_col = inv_col + 3
    elif rozkhid_col is not None:
        price_col = rozkhid_col + 4
    else:
        price_col = 11  # резервна позиція
    for i2 in range(data_start, len(df)):
        r2 = df.iloc[i2]
        if r2.iloc[0] == '+' and is_article_code(str(r2.iloc[1]).strip()):
            try:
                p = float(r2.iloc[price_col])
                if p > 0:
                    prices[str(r2.iloc[1]).strip()] = p
            except Exception:
                # Спробувати стандартну позицію
                try:
                    p = float(r2.iloc[11])
                    if p > 0:
                        prices[str(r2.iloc[1]).strip()] = p
                except Exception:
                    pass

    return header, pd.DataFrame(data), prices
