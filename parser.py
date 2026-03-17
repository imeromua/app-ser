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

# Код магазину (розхід при Ппт). Перевіряємо обидва варіанти X — латинська і кирилична Х
SHOP_CODE = 'X016'
SHOP_CODE_CYR = '\u0425016'  # кирилична Х + 016


def is_article_code(val):
    s = str(val).strip()
    return s.isdigit() and len(s) >= 5


def _is_empty_text(text: str) -> bool:
    """Повертає True, якщо текст є відсутнім або порожнім замінником."""
    return not text or text in ('nan', '-')


def _is_own_shop(doc_text: str) -> bool:
    """
    Повертає True якщо документ Ппт стосується власного магазину (розхід).
    Ппт/X016... або Ппт/Х016... — це РОЗХІД (переміщення з магазину).
    Ппт/<будь-що інше> — це ПРИХІД (переміщення в магазин).
    Перевіряємо обидва варіанти X: латинська ASCII та кирилична Unicode.
    """
    # Витягуємо частину після /
    m = re.search(r'[Пп][Пп][Тт][/\\](\S+)', doc_text)
    if not m:
        # Якщо / немає — перевіряємо чи є код магазину взагалі в тексті
        t = doc_text.upper()
        return SHOP_CODE.upper() in t or SHOP_CODE_CYR.upper() in t
    after_slash = m.group(1).upper()
    # Нормалізуємо кириличну Х -> латинську X для порівняння
    after_slash_norm = after_slash.replace('\u0425', 'X')
    shop_norm = SHOP_CODE.upper().replace('\u0425', 'X')
    return after_slash_norm.startswith(shop_norm)


def _classify(doc_text: str, raw_qty: float):
    """Класифікує операцію ВИКЛЮЧНО на основі тексту документа."""
    abs_qty = abs(raw_qty)
    op = '\u0406\u043dше'
    pryhid_val, rozkhid_val, inv_val = 0.0, 0.0, 0.0

    if '\u041a\u043d\u043a' in doc_text:
        op = '\u041a\u043d\u043a (\u041f\u0440\u043e\u0434\u0430\u0436\u0456)'
        rozkhid_val = abs_qty
    elif '\u0421\u043f\u041e' in doc_text or '\u0421\u043f\u041f' in doc_text:
        op = '\u0421\u043f\u041f (\u0421\u043f\u0438\u0441\u0430\u043d\u043d\u044f)'
        rozkhid_val = abs_qty
    elif '\u041f\u0440\u0418' in doc_text:
        op = '\u041f\u0440\u0418 (\u041f\u0435\u0440\u0435\u043c\u0456\u0449\u0435\u043d\u043d\u044f)'
        rozkhid_val = abs_qty
    elif '\u0410\u043f\u043a' in doc_text or '\u0410\u043f\u0441' in doc_text:
        op = '\u0410\u043f\u0441 (\u0410\u043a\u0442 \u043f\u0435\u0440\u0435\u0441\u043e\u0440\u0442\u0443)'
        inv_val = raw_qty
    elif '\u041f\u0440\u0412' in doc_text:
        op = '\u041f\u0440\u0412 (\u041f\u0440\u0438\u0445\u0456\u0434)'
        pryhid_val = abs_qty
    elif re.search(r'[\u041f\u043f][\u041f\u043f][\u0422\u0442]', doc_text):
        # П\u043f\u0442 (п\u0435\u0440\u0435\u043c\u0456\u0449\u0435\u043d\u043d\u044f \u043c\u0456\u0436 \u043c\u0430\u0433\u0430\u0437\u0438\u043d\u0430\u043c\u0438)
        # П\u043f\u0442/X016... \u2014 \u0446\u0435 \u0420\u041e\u0417\u0425\u041e\u0414 (\u043f\u0435\u0440\u0435\u043c\u0456\u0449\u0435\u043d\u043d\u044f \u0437 \u0446\u044c\u043e\u0433\u043e \u043c\u0430\u0433\u0430\u0437\u0438\u043d\u0443)
        # П\u043f\u0442/<\u0456\u043d\u0448\u0438\u0439> \u2014 \u0446\u0435 ПРИХІД (\u043f\u0435\u0440\u0435\u043c\u0456\u0449\u0435\u043d\u043d\u044f \u0437 \u0456\u043d\u0448\u043e\u0433\u043e \u043c\u0430\u0433\u0430\u0437\u0438\u043d\u0443)
        if _is_own_shop(doc_text):
            op = '\u041f\u0440\u0418 (\u041f\u0435\u0440\u0435\u043c\u0456\u0449\u0435\u043d\u043d\u044f)'
            rozkhid_val = abs_qty
        else:
            op = '\u041f\u0440\u0412 (\u041f\u0440\u0438\u0445\u0456\u0434)'
            pryhid_val = abs_qty
    else:
        if raw_qty > 0:
            op = '\u041f\u0440\u0412 (\u041f\u0440\u0438\u0445\u0456\u0434)'
            pryhid_val = abs_qty
        else:
            op = '\u0421\u043f\u041f (\u0421\u043f\u0438\u0441\u0430\u043d\u043d\u044f)'
            rozkhid_val = abs_qty

    return op, pryhid_val, rozkhid_val, inv_val


def parse_xls(buf):
    buf.seek(0)
    header_bytes = buf.read(4)
    if not any(header_bytes.startswith(sig) for sig in ALLOWED_MIME_HEADERS):
        raise ValueError('\u041d\u0435\u0432\u0456\u0440\u043d\u0438\u0439 \u0442\u0438\u043f \u0444\u0430\u0439\u043b\u0443: \u0434\u043e\u0437\u0432\u043e\u043b\u0435\u043d\u043e \u043b\u0438\u0448\u0435 .xls \u0442\u0430 .xlsx')

    buf.seek(0)
    df = pd.read_excel(buf, sheet_name=0, header=None)

    header = {
        'title': '\u0420\u0443\u0445 \u0442\u043e\u0432\u0430\u0440\u0456\u0432 \u043a\u0456\u043b\u044c\u043a\u0456\u0441\u043d\u0438\u0439',
        'shop': '',
        'period': '',
        'warehouse': ''
    }

    for r in range(min(15, len(df))):
        for c in range(len(df.columns)):
            val = str(df.iloc[r, c]).strip()
            if not val or val == 'nan': continue

            if '\u041c\u0430\u0433\u0430\u0437\u0438\u043d:' in val:
                if len(val) > 10: header['shop'] = val.replace('\u041c\u0430\u0433\u0430\u0437\u0438\u043d:', '').strip()
                else:
                    for nc in range(c+1, len(df.columns)):
                        nval = str(df.iloc[r, nc]).strip()
                        if nval and nval != 'nan' and nval != '-':
                            header['shop'] = nval; break

            elif '\u0421\u043a\u043b\u0430\u0434:' in val:
                if len(val) > 8: header['warehouse'] = val.replace('\u0421\u043a\u043b\u0430\u0434:', '').strip()
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
        col1   = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''

        if marker == '+':
            if is_article_code(col1):
                cur_art  = col1
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
                cur_art  = col1
                cur_name = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else cur_name
                i += 1
                continue

            if not cur_art:
                i += 1
                continue

            doc_text = col1

            raw_qty = 0.0
            for col_idx in range(4, min(15, len(row))):
                v = row.iloc[col_idx]
                if pd.notna(v) and str(v).strip() != '':
                    try:
                        raw_qty = float(v)
                        break
                    except ValueError:
                        pass

            if raw_qty == 0.0 or _is_empty_text(doc_text):
                for offset in (-1, 1, 2):
                    idx = i + offset
                    if not (0 <= idx < len(df)):
                        continue
                    adj_row    = df.iloc[idx]
                    adj_marker = str(adj_row.iloc[0]).strip() if len(adj_row) > 0 and pd.notna(adj_row.iloc[0]) else ''
                    adj_col1   = str(adj_row.iloc[1]).strip() if len(adj_row) > 1 and pd.notna(adj_row.iloc[1]) else ''

                    if offset > 0 and adj_marker in ('+', '-') and is_article_code(adj_col1):
                        break

                    adj_qty = 0.0
                    for ci in range(4, min(15, len(adj_row))):
                        v = adj_row.iloc[ci]
                        if pd.notna(v) and str(v).strip() != '':
                            try:
                                adj_qty = float(v)
                                break
                            except ValueError:
                                pass

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
                '\u0410\u0440\u0442\u0438\u043a\u0443\u043b':    cur_art,
                '\u041d\u0430\u0437\u0432\u0430':      cur_name,
                '\u041e\u043f\u0435\u0440\u0430\u0446\u0456\u044f':   op,
                '\u0414\u043e\u043a\u0443\u043c\u0435\u043d\u0442':  doc_text,
                '\u0414\u0430\u0442\u0430':       last_date,
                '\u0420\u0456\u043a-\u041c\u0456\u0441\u044f\u0446\u044c': ym,
                '\u041a\u0456\u043b\u044c\u043a\u0456\u0441\u0442\u044c': qty,
                '\u041f\u0440\u0438\u0445\u0456\u0434':    pryhid_val,
                '\u0420\u043e\u0437\u0445\u0456\u0434':    rozkhid_val,
                '\u0406\u043d\u0432':       inv_val
            })
        i += 1

    return header, pd.DataFrame(data), prices
