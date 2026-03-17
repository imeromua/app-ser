"""
parser.py — парсинг XLS-файлів звіту EPI.
"""

import logging
import re

import pandas as pd

DOC_PREFIXES = ('ПрВ', 'Кнк', 'СпП', 'СпО', 'ПрИ', 'Апс')

ALLOWED_MIME_HEADERS = (
    b'\xd0\xcf\x11\xe0',  # .xls  (OLE2)
    b'PK\x03\x04',        # .xlsx (ZIP/OOXML)
)


def is_article_code(val):
    s = str(val).strip()
    return s.isdigit() and len(s) >= 5


def find_data_start(df):
    for i in range(5, min(40, len(df))):
        try:
            if df.iloc[i, 0] == '+' and is_article_code(str(df.iloc[i, 1]).strip()):
                return i
        except Exception:
            continue
    return 15  # fallback


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

    data, i = [], find_data_start(df)
    cur_art, cur_name = None, ''

    while i < len(df):
        row    = df.iloc[i]
        marker = row[0]
        col1   = str(row[1]).strip() if pd.notna(row[1]) else ''

        if marker == '+' and is_article_code(col1):
            cur_art  = col1
            cur_name = str(row[2]).strip() if pd.notna(row[2]) else ''
            i += 1
            continue

        if marker == '-':
            if is_article_code(col1):
                cur_art  = col1
                cur_name = str(row[2]).strip() if pd.notna(row[2]) else cur_name
                i += 1
                continue

            if col1.startswith(DOC_PREFIXES) and cur_art:
                if   col1.startswith('ПрВ'): op, qty = 'ПрВ', row[6]
                elif col1.startswith('Кнк'): op, qty = 'Кнк', row[7]
                elif col1.startswith('СпП'): op, qty = 'СпП', row[6]
                elif col1.startswith('СпО'): op, qty = 'СпО', row[6]
                elif col1.startswith('ПрИ'): op, qty = 'ПрИ', row[6]
                elif col1.startswith('Апс'): op, qty = 'Апс', row[8]
                else:                         op, qty = 'Інше', row[6]

                qty = float(qty) if pd.notna(qty) else 0
                m = re.search(r'(\d{2}\.\d{2}\.\d{2})', col1)
                if m:
                    try:
                        d = pd.to_datetime(m.group(1), format='%d.%m.%y')
                        data.append({
                            'Артикул': cur_art, 'Назва': cur_name, 'Операція': op,
                            'Дата': d, 'Рік-Місяць': f"{d.year}-{d.month:02d}", 'Кількість': qty
                        })
                    except Exception as e:
                        logging.warning(f"Рядок {i} пропущено: {e}")
        i += 1

    prices = {}
    start = find_data_start(df)
    for i2 in range(start, len(df)):
        r2 = df.iloc[i2]
        if r2[0] == '+' and is_article_code(str(r2[1]).strip()):
            try:
                p = float(r2[11])
                if p > 0:
                    prices[str(r2[1]).strip()] = p
            except Exception:
                pass

    return header, pd.DataFrame(data), prices
