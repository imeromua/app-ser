"""
parser.py — парсинг XLS/XLSX-файлів звіту EPI.
"""

import logging
import re
import pandas as pd

ALLOWED_MIME_HEADERS = (
    b'\xd0\xcf\x11\xe0',  # .xls  (OLE2)
    b'PK\x03\x04',        # .xlsx (ZIP/OOXML)
)

def is_article_code(val):
    s = str(val).strip()
    return s.isdigit() and len(s) >= 5

def _is_empty_text(text: str) -> bool:
    """Повертає True, якщо текст є відсутнім або порожнім замінником."""
    return not text or text in ('nan', '-')

def _classify(doc_text: str, raw_qty: float):
    """Класифікує операцію ВИКЛЮЧНО на основі тексту документа."""
    abs_qty = abs(raw_qty)
    op = 'Інше'
    pryhid_val, rozkhid_val, inv_val = 0.0, 0.0, 0.0

    if 'Кнк' in doc_text:
        op = 'Кнк (Продажі)'
        rozkhid_val = abs_qty
    elif 'СпО' in doc_text or 'СпП' in doc_text:
        op = 'СпП (Списання)'
        rozkhid_val = abs_qty
    elif 'ПрИ' in doc_text:
        op = 'ПрИ (Переміщення)'
        rozkhid_val = abs_qty
    elif 'Апк' in doc_text or 'Апс' in doc_text:
        op = 'Апс (Акт пересорту)'
        inv_val = raw_qty
    elif 'ПрВ' in doc_text:
        op = 'ПрВ (Прихід)'
        pryhid_val = abs_qty
    elif 'Ппт' in doc_text:
        if 'X016' in doc_text:
            op = 'ПрИ (Переміщення)'
            rozkhid_val = abs_qty
        else:
            op = 'ПрВ (Прихід)'
            pryhid_val = abs_qty
    else:
        if raw_qty > 0:
            op = 'ПрВ (Прихід)'
            pryhid_val = abs_qty
        else:
            op = 'СпП (Списання)'
            rozkhid_val = abs_qty

    return op, pryhid_val, rozkhid_val, inv_val

def parse_xls(buf):
    buf.seek(0)
    header_bytes = buf.read(4)
    if not any(header_bytes.startswith(sig) for sig in ALLOWED_MIME_HEADERS):
        raise ValueError('Невірний тип файлу: дозволено лише .xls та .xlsx')
    
    buf.seek(0)
    df = pd.read_excel(buf, sheet_name=0, header=None)

    header = {
        'title': 'Рух товарів кількісний',
        'shop': '',
        'period': '',
        'warehouse': ''
    }
    
    for r in range(min(15, len(df))):
        for c in range(len(df.columns)):
            val = str(df.iloc[r, c]).strip()
            if not val or val == 'nan': continue
            
            if 'Магазин:' in val:
                if len(val) > 10: header['shop'] = val.replace('Магазин:', '').strip()
                else:
                    for nc in range(c+1, len(df.columns)):
                        nval = str(df.iloc[r, nc]).strip()
                        if nval and nval != 'nan' and nval != '-':
                            header['shop'] = nval; break
                            
            elif 'Склад:' in val:
                if len(val) > 8: header['warehouse'] = val.replace('Склад:', '').strip()
                else:
                    for nc in range(c+1, len(df.columns)):
                        nval = str(df.iloc[r, nc]).strip()
                        if nval and nval != 'nan' and nval != '-':
                            header['warehouse'] = nval; break
                            
            if re.search(r'\d{2}\.\d{2}\.\d{2,4}\s+\d{1,2}:\d{2}', val):
                header['period'] = val

    data = []
    prices = {}
    cur_art, cur_name = None, ''
    last_date = None

    data_start = 10
    for i in range(min(50, len(df))):
        try:
            if str(df.iloc[i, 0]).strip() == '+' and is_article_code(str(df.iloc[i, 1]).strip()):
                data_start = i
                break
        except Exception: pass

    i = data_start
    while i < len(df):
        row = df.iloc[i]
        marker = str(row.iloc[0]).strip() if len(row) > 0 and pd.notna(row.iloc[0]) else ''
        col1 = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''

        if marker == '+':
            if is_article_code(col1):
                cur_art = col1
                cur_name = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ''
                for c_idx in range(len(row)-1, 5, -1):
                    val = row.iloc[c_idx]
                    if pd.notna(val):
                        try:
                            p = float(val)
                            if p > 0: prices[cur_art] = p; break
                        except: pass
            i += 1
            continue

        if marker == '-':
            if is_article_code(col1):
                cur_art = col1
                cur_name = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else cur_name
                i += 1
                continue

            if not cur_art:
                i += 1
                continue

            doc_text = col1

            # Збір raw_qty: перше знайдене число в клітинках 4–14
            raw_qty = 0.0
            for col_idx in range(4, min(15, len(row))):
                v = row.iloc[col_idx]
                if pd.notna(v) and str(v).strip() != '':
                    try:
                        raw_qty = float(v)
                        break
                    except ValueError:
                        pass

            # Зшивання: якщо поточний рядок не має цифр або не має тексту документа,
            # заглядаємо на 1 рядок вгору та на 2 вниз
            if raw_qty == 0.0 or _is_empty_text(doc_text):
                for offset in (-1, 1, 2):
                    idx = i + offset
                    if not (0 <= idx < len(df)):
                        continue
                    adj_row = df.iloc[idx]
                    adj_marker = str(adj_row.iloc[0]).strip() if len(adj_row) > 0 and pd.notna(adj_row.iloc[0]) else ''
                    adj_col1 = str(adj_row.iloc[1]).strip() if len(adj_row) > 1 and pd.notna(adj_row.iloc[1]) else ''

                    # Не перетинаємо межу нового товару або операції
                    if offset > 0 and adj_marker in ('+', '-') and is_article_code(adj_col1):
                        break

                    # Шукаємо цифри в сусідньому рядку
                    adj_qty = 0.0
                    for ci in range(4, min(15, len(adj_row))):
                        v = adj_row.iloc[ci]
                        if pd.notna(v) and str(v).strip() != '':
                            try:
                                adj_qty = float(v)
                                break
                            except ValueError:
                                pass

                    # Намагаємось склеїти текст
                    adj_text = adj_col1 if not _is_empty_text(adj_col1) and adj_col1 != '+' else ''
                    if not adj_text and adj_marker not in ('nan', '-', '+', ''):
                        adj_text = adj_marker

                    if raw_qty == 0.0 and adj_qty != 0.0:
                        raw_qty = adj_qty
                        if adj_text and _is_empty_text(doc_text):
                            doc_text = adj_text
                        elif adj_text:
                            doc_text = (adj_text + ' ' + doc_text) if offset < 0 else (doc_text + ' ' + adj_text)
                        break
                    elif adj_text and _is_empty_text(doc_text):
                        doc_text = adj_text

            if raw_qty == 0:
                i += 1
                continue

            op, pryhid_val, rozkhid_val, inv_val = _classify(doc_text, raw_qty)
            qty = pryhid_val - rozkhid_val + inv_val

            m = re.search(r'(\d{2}\.\d{2}\.\d{2})', doc_text)
            if m:
                try: last_date = pd.to_datetime(m.group(1), format='%d.%m.%y')
                except Exception: pass

            ym = f"{last_date.year}-{last_date.month:02d}" if last_date else ''

            data.append({
                'Артикул':   cur_art,
                'Назва':     cur_name,
                'Операція':  op,
                'Документ':  doc_text,
                'Дата':      last_date,
                'Рік-Місяць': ym,
                'Кількість': qty,
                'Прихід':    pryhid_val,
                'Розхід':    rozkhid_val,
                'Інв':       inv_val
            })
        i += 1

    return header, pd.DataFrame(data), prices