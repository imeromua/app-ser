"""
parser.py — парсинг XLS/XLSX-файлів звіту EPI.

Ця версія використовує "універсальний сканер": замість жорсткої прив'язки 
до номерів колонок (які можуть зміщуватися залежно від формату вивантаження), 
парсер шукає числа у відповідних зонах рядка і самостійно визначає, 
що це за операція, спираючись виключно на назву документа.
"""

import logging
import re
import pandas as pd

# Дозволені сигнатури файлів (захист від завантаження не Excel файлів)
ALLOWED_MIME_HEADERS = (
    b'\xd0\xcf\x11\xe0',  # .xls  (OLE2 - старий формат)
    b'PK\x03\x04',        # .xlsx (ZIP/OOXML - новий формат)
)

def is_article_code(val):
    """Перевіряє, чи є значення валідним артикулом (лише цифри, від 5 знаків)."""
    s = str(val).strip()
    return s.isdigit() and len(s) >= 5

def _classify_and_route(doc_text: str, raw_qty: float):
    """
    Головний мозок маршрутизації кількості.
    Аналізує текст документа і вирішує, куди записати знайдену кількість:
    у Прихід (плюс), Розхід (мінус) чи Інвентаризацію (корегування).
    """
    abs_qty = abs(raw_qty)
    op = 'Інше'
    pryhid_val, rozkhid_val, inv_val = 0.0, 0.0, 0.0

    # Перевіряємо ключові слова у назві документа
    if 'Ппт' in doc_text:
        # Якщо в документі є наш код складу (X016) — це ми віддаємо (Розхід)
        if 'Ппт/X016' in doc_text:
            op = 'Ппт (Переміщення Розхід)'
            rozkhid_val = abs_qty
        # Якщо інший код — це нам привезли (Прихід)
        else:
            op = 'Ппт (Переміщення Прихід)'
            pryhid_val = abs_qty
    elif 'Кнк' in doc_text:
        op = 'Кнк (Продаж)'
        rozkhid_val = abs_qty
    elif 'СпО' in doc_text or 'СпП' in doc_text:
        op = 'СпП (Списання)'
        rozkhid_val = abs_qty
    elif 'Апк' in doc_text or 'Апс' in doc_text:
        op = 'Апк (Корегування)'
        inv_val = raw_qty  # Тут зберігаємо оригінальний знак (+ або -)
    elif 'ПрИ' in doc_text:
        op = 'ПрИ (Переміщення)'
        rozkhid_val = abs_qty
    elif 'ПрВ' in doc_text:
        op = 'ПрВ (Прихід)'
        pryhid_val = abs_qty
    else:
        # Резервне правило: якщо документ не розпізнано, дивимось на знак числа
        if raw_qty > 0: 
            pryhid_val = raw_qty
        else: 
            rozkhid_val = abs_qty

    return op, pryhid_val, rozkhid_val, inv_val

def parse_xls(buf):
    # 1. Перевірка формату файлу за магічними байтами
    buf.seek(0)
    header_bytes = buf.read(4)
    if not any(header_bytes.startswith(sig) for sig in ALLOWED_MIME_HEADERS):
        raise ValueError('Невірний тип файлу: дозволено лише .xls та .xlsx')
    
    # 2. Читання файлу через pandas (без заголовків, щоб бачити всю структуру)
    buf.seek(0)
    df = pd.read_excel(buf, sheet_name=0, header=None)

    def cell(r, c):
        """Безпечне отримання тексту з клітинки."""
        try:
            v = df.iloc[r, c]
            return str(v).strip() if pd.notna(v) else ''
        except Exception:
            return ''

    # 3. Витягування метаданих (шапка звіту)
    # Перевіряємо сусідні клітинки на випадок зміщення
    header = {
        'title':     cell(0, 0),
        'shop':      cell(1, 3) if cell(1, 3) else cell(1, 2),
        'period':    cell(1, 10) if cell(1, 10) else cell(1, 9),
        'warehouse': cell(3, 3) if cell(3, 3) else cell(3, 2),
    }

    data = []
    prices = {}
    cur_art, cur_name = None, ''
    last_date = None

    # 4. Пошук першого рядка з даними (там де є '+' і артикул)
    data_start = 10
    for i in range(min(50, len(df))):
        try:
            if str(df.iloc[i, 0]).strip() == '+' and is_article_code(str(df.iloc[i, 1]).strip()):
                data_start = i
                break
        except Exception:
            pass

    # 5. Основний цикл обробки рядків
    i = data_start
    while i < len(df):
        row = df.iloc[i]
        marker = str(row.iloc[0]).strip() if len(row) > 0 and pd.notna(row.iloc[0]) else ''
        col1 = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''

        # --- ОБРОБКА РЯДКА АРТИКУЛУ (+) ---
        if marker == '+':
            if is_article_code(col1):
                cur_art = col1
                cur_name = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ''
                
                # Шукаємо ціну: вона завжди є останнім числом у правій частині таблиці
                # Тому скануємо рядок з кінця до початку (від останньої колонки до 5-ї)
                for c_idx in range(len(row)-1, 5, -1):
                    val = row.iloc[c_idx]
                    if pd.notna(val):
                        try:
                            p = float(val)
                            if p > 0:
                                prices[cur_art] = p
                                break  # Знайшли ціну — зупиняємо пошук
                        except:
                            pass
            i += 1
            continue

        # --- ОБРОБКА ТЕХНІЧНОГО РЯДКА (-) ---
        if marker == '-':
            if is_article_code(col1):
                cur_art = col1
                cur_name = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else cur_name
                i += 1
                continue

            # Якщо це просто мінус без артикулу (наприклад, між документами) — пропускаємо
            if not cur_art:
                i += 1
                continue

            doc_text = col1

            # --- СКЛЕЮВАННЯ ЗДВОЄНИХ ДОКУМЕНТІВ (Lookahead) ---
            # Іноді система розбиває складний документ (напр. ПрВ/...-Ппт/...) на два рядки.
            # Ми заглядаємо на 1-2 рядки вперед, щоб знайти "хвіст" документа.
            for lookahead in range(1, 3):
                if i + lookahead < len(df):
                    next_row = df.iloc[i + lookahead]
                    next_marker = str(next_row.iloc[0]).strip() if len(next_row) > 0 and pd.notna(next_row.iloc[0]) else ''
                    next_col1 = str(next_row.iloc[1]).strip() if len(next_row) > 1 and pd.notna(next_row.iloc[1]) else ''

                    # Якщо натрапили на новий товар — зупиняємо пошук
                    if next_marker in ('+', '-'):
                        break

                    supp_text = ''
                    if len(next_marker) > 5 and ('/' in next_marker or 'Ппт' in next_marker):
                        supp_text = next_marker
                    elif len(next_col1) > 5 and ('/' in next_col1 or 'Ппт' in next_col1):
                        supp_text = next_col1

                    if supp_text:
                        # Перевіряємо, чи в наступному рядку немає своїх цифр, 
                        # щоб випадково не приклеїти іншу повноцінну операцію
                        n_qty = 0.0
                        for ci in range(4, min(15, len(next_row))):
                            v = next_row.iloc[ci]
                            if pd.notna(v) and str(v).strip() != '':
                                try: n_qty += float(v)
                                except: pass
                        if n_qty == 0:
                            doc_text += ' ' + supp_text # Склеюємо текст

            # --- УНІВЕРСАЛЬНИЙ ПОШУК КІЛЬКОСТІ ---
            # Не шукаємо конкретну колонку "Розхід". Просто скануємо всі комірки від 4 до 15.
            # Оскільки в рядку операції завжди є лише одне значуще число, ми його гарантовано знайдемо.
            raw_qty = 0.0
            for col_idx in range(4, min(15, len(row))):
                v = row.iloc[col_idx]
                if pd.notna(v) and str(v).strip() != '':
                    try:
                        raw_qty += float(v)
                    except ValueError:
                        pass

            # Якщо цифр не знайдено — це пустий рядок (можливо текст), пропускаємо
            if raw_qty == 0:
                i += 1
                continue

            # --- МАРШРУТИЗАЦІЯ ТА ПІДРАХУНОК ---
            # Віддаємо знайдений текст і кількість нашій функції для класифікації
            op, pryhid_val, rozkhid_val, inv_val = _classify_and_route(doc_text, raw_qty)
            
            # Математична дельта операції: скільки реально додалося чи віднялося
            qty = pryhid_val - rozkhid_val + inv_val

            # Витягуємо дату з тексту документа
            m = re.search(r'(\d{2}\.\d{2}\.\d{2})', doc_text)
            if m:
                try:
                    last_date = pd.to_datetime(m.group(1), format='%d.%m.%y')
                except Exception:
                    pass

            ym = f"{last_date.year}-{last_date.month:02d}" if last_date else ''

            # Записуємо повністю готову та класифіковану операцію
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