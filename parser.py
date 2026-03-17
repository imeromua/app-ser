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

def _classify_and_route(doc_text: str, raw_qty: float):
    """
    Класифікує документ та віддає точні назви операцій,
    які очікує builder.py для правильного рознесення по колонках.
    """
    abs_qty = abs(raw_qty)
    op = 'Інше'
    pryhid_val, rozkhid_val, inv_val = 0.0, 0.0, 0.0

    # Пріоритет перевірки: спочатку основні документи
    if 'Кнк' in doc_text:
        op = 'Кнк (Продажі)'
        rozkhid_val = abs_qty
    elif 'СпО' in doc_text or 'СпП' in doc_text:
        op = 'СпП (Списання)'
        rozkhid_val = abs_qty
    elif 'Апк' in doc_text or 'Апс' in doc_text:
        op = 'Апс (Акт пересорту)'
        inv_val = raw_qty
    elif 'ПрВ' in doc_text:
        op = 'ПрВ (Прихід)'
        pryhid_val = abs_qty
    elif 'ПрИ' in doc_text:
        op = 'ПрИ (Переміщення)'
        rozkhid_val = abs_qty
    elif 'Ппт' in doc_text:
        if 'Ппт/X016' in doc_text:
            op = 'ПрИ (Переміщення)'  # Ппт/X016 - це наш розхід
            rozkhid_val = abs_qty
        else:
            op = 'ПрВ (Прихід)'       # Інші Ппт - це прихід до нас
            pryhid_val = abs_qty
    else:
        # Резерв
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

    # --- 1. РОЗУМНИЙ ПОШУК ШАПКИ (Броньований від зсувів) ---
    header = {
        'title': 'Рух товарів кількісний',
        'shop': '',
        'period': '',
        'warehouse': ''
    }
    
    for r in range(min(15, len(df))):
        for c in range(len(df.columns)):
            val = str(df.iloc[r, c]).strip()
            if not val or val == 'nan':
                continue
            
            # Шукаємо магазин
            if 'Магазин:' in val:
                if len(val) > 10: 
                    header['shop'] = val.replace('Магазин:', '').strip()
                else:
                    for nc in range(c+1, len(df.columns)):
                        nval = str(df.iloc[r, nc]).strip()
                        if nval and nval != 'nan' and nval != '-':
                            header['shop'] = nval
                            break
                            
            # Шукаємо склад
            elif 'Склад:' in val:
                if len(val) > 8: 
                    header['warehouse'] = val.replace('Склад:', '').strip()
                else:
                    for nc in range(c+1, len(df.columns)):
                        nval = str(df.iloc[r, nc]).strip()
                        if nval and nval != 'nan' and nval != '-':
                            header['warehouse'] = nval
                            break
                            
            # Шукаємо період (формат дати)
            if re.search(r'\d{2}\.\d{2}\.\d{2,4}\s+\d{1,2}:\d{2}', val):
                header['period'] = val

    data = []
    prices = {}
    cur_art, cur_name = None, ''
    last_date = None

    # --- 2. ПОШУК ПОЧАТКУ ДАНИХ ---
    data_start = 10
    for i in range(min(50, len(df))):
        try:
            if str(df.iloc[i, 0]).strip() == '+' and is_article_code(str(df.iloc[i, 1]).strip()):
                data_start = i
                break
        except Exception:
            pass

    # --- 3. ПАРСИНГ РЯДКІВ ---
    i = data_start
    while i < len(df):
        row = df.iloc[i]
        marker = str(row.iloc[0]).strip() if len(row) > 0 and pd.notna(row.iloc[0]) else ''
        col1 = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''

        if marker == '+':
            if is_article_code(col1):
                cur_art = col1
                cur_name = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ''
                
                # Пошук ціни з кінця рядка
                for c_idx in range(len(row)-1, 5, -1):
                    val = row.iloc[c_idx]
                    if pd.notna(val):
                        try:
                            p = float(val)
                            if p > 0:
                                prices[cur_art] = p
                                break
                        except:
                            pass
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

            # Зшивання розірваних документів
            for lookahead in range(1, 3):
                if i + lookahead < len(df):
                    next_row = df.iloc[i + lookahead]
                    next_marker = str(next_row.iloc[0]).strip() if len(next_row) > 0 and pd.notna(next_row.iloc[0]) else ''
                    next_col1 = str(next_row.iloc[1]).strip() if len(next_row) > 1 and pd.notna(next_row.iloc[1]) else ''

                    if next_marker in ('+', '-'):
                        break

                    supp_text = ''
                    if len(next_marker) > 5 and ('/' in next_marker or 'Ппт' in next_marker):
                        supp_text = next_marker
                    elif len(next_col1) > 5 and ('/' in next_col1 or 'Ппт' in next_col1):
                        supp_text = next_col1

                    if supp_text:
                        n_qty = 0.0
                        for ci in range(4, min(15, len(next_row))):
                            v = next_row.iloc[ci]
                            if pd.notna(v) and str(v).strip() != '':
                                try: n_qty += float(v)
                                except: pass
                        if n_qty == 0:
                            doc_text += ' ' + supp_text

            # Збір кількості (беремо тільки реальну кількість операції)
            raw_qty = 0.0
            for col_idx in range(4, min(15, len(row))):
                v = row.iloc[col_idx]
                if pd.notna(v) and str(v).strip() != '':
                    try:
                        raw_qty += float(v)
                    except ValueError:
                        pass

            if raw_qty == 0:
                i += 1
                continue

            op, pryhid_val, rozkhid_val, inv_val = _classify_and_route(doc_text, raw_qty)
            qty = pryhid_val - rozkhid_val + inv_val

            m = re.search(r'(\d{2}\.\d{2}\.\d{2})', doc_text)
            if m:
                try:
                    last_date = pd.to_datetime(m.group(1), format='%d.%m.%y')
                except Exception:
                    pass

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