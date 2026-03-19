"""
builder.py — побудова рядків для детального, сумарного та документального звітів.
"""

# ── Відображення типу операції на ключ агрегованого стовпця ──────────────────
# УВАГА: ключі МАЮТЬ точно збігатися з рядками, що повертає _classify_operation() у parser.py
_OP_TO_COL = {
    'ПрВ (Прихід)':                'ПрВ',
    'Кнк (Продаж)':                'Кнк',
    'ПрИ (Переміщення)':           'ПрИ',
    'Ппт (Переміщення Прихід)':    'ПрИ',
    'Ппт (Переміщення Розхід)':    'ПрИ',
    'СпП (Списання)':              'СпП',
    'Апк (Корегування)':           'Апс',
}

# Операції, що є ПРИХОДОМ у звіті звіті «По документах»
_OP_IS_PRYHID = {
    'ПрВ (Прихід)',             # Прямий прихід
    'Ппт (Переміщення Прихід)',  # Ппт з іншого магазину — приходить до нас
}

# Операції, що є РОЗХОДОМ у звіті «По документах»
_OP_IS_ROZKHID = {
    'Кнк (Продаж)',              # Продаж
    'ПрИ (Переміщення)',          # Пряме переміщення (ПрИ)
    'Ппт (Переміщення Розхід)',   # Ппт/X016 — іде з нашого магазину
    'СпП (Списання)',             # Списання
}
# Апк (Корегування) — може бути + або −, визначаємо по знаку qty


# Precision for weighted-item quantities (decimal places shown in UI and used in rounding)
_QTY_PRECISION = 3


def _is_weighted(qty_series) -> bool:
    """True якщо хоч одне qty в серії має дробову частину — товар є ваговим (кг)."""
    for q in qty_series:
        try:
            v = float(q)
            if abs(v - round(v)) > 1e-9:
                return True
        except (TypeError, ValueError):
            pass
    return False


def _doc_pryhid_rozkhid(op_name: str, qty: float):
    """
    Повертає (pryhid_val, rozkhid_val) для звіту «По документах».
    Логіка визначення по назві операції, а не по знаку qty:
      - ПрВ, Ппт(Прихід)                → ПРИХІД
      - Кнк, ПрИ, Ппт(Розхід), СпП  → РОЗХІД
      - Апк (корегування)           → знак по qty (+ прихід, − розхід)
    """
    abs_qty = abs(qty)
    if op_name in _OP_IS_PRYHID:
        return abs_qty, 0.0
    if op_name in _OP_IS_ROZKHID:
        return 0.0, abs_qty
    # Апк / Інше — по знаку
    if qty >= 0:
        return abs_qty, 0.0
    return 0.0, abs_qty


def _agg_cols(df_slice):
    """
    Повертає dict з агрегованими значеннями для стовпців ПрВ, Кнк, ПрИ, СпП, Апс.

    Правила знаків:
      - ПрВ, Кнк, СпП → абсолютне значення (abs), завжди ≥ 0
      - ПрИ, Апс       → знакова сума, може бути від'ємною
        (наприклад, Апс = -4 означає нестачу при пересорті)

    Значення зберігаються як float з точністю до 3 знаків, щоб не втрачати
    дробову частину вагових товарів (наприклад 2.286 кг).
    """
    totals = {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0}
    for op, col in _OP_TO_COL.items():
        mask = df_slice['Операція'] == op
        s = df_slice.loc[mask, 'Кількість'].sum()
        if col in ('ПрВ', 'Кнк', 'СпП'):
            # Ці колонки завжди відображаються як абсолютна величина
            totals[col] += abs(s)
        else:
            # ПрИ та Апс — знакові: від'ємне значення є значущим
            totals[col] += s
    return {k: round(float(v), _QTY_PRECISION) for k, v in totals.items()}


def build_rows(ops_df, prices, balance_starts=None):
    """Детальний звіт — розбивка по місяцях для кожного артикулу.

    balance_starts: dict {article_id → float} — залишок на початок періоду
                    (col E з XLS).  Якщо None або відсутній для артикулу — 0.
    Залишок у рядку РАЗОМ = balance_start + SUM(qty за період) = кінцевий залишок.
    Залишок у місячних рядках = нетто-зміна за місяць (без balance_start).
    """
    if ops_df.empty:
        return [], {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0, 'Залишок': 0, 'Сума': 0.0}

    _bs = balance_starts or {}

    articles = ops_df.groupby('Артикул')['Назва'].first().reset_index()
    months   = sorted(ops_df['Рік-Місяць'].dropna().unique())
    rows     = []
    grand    = {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0, 'Залишок': 0, 'Сума': 0.0}

    for _, ar in articles.iterrows():
        art, name = ar['Артикул'], ar['Назва']
        df_a = ops_df[ops_df['Артикул'] == art]
        is_w = _is_weighted(df_a['Кількість'])
        bs   = float(_bs.get(art, 0.0))
        mrows = []

        for month in months:
            dm = df_a[df_a['Рік-Місяць'] == month]
            if dm.empty:
                continue
            cols = _agg_cols(dm)
            if all(v == 0 for v in cols.values()):
                continue
            zal = round(float(dm['Кількість'].sum()), 4)
            mrows.append({'type': 'data', 'Артикул': art, 'Назва': name, 'Місяць': month,
                          **cols, 'Залишок': zal, 'Ціна': '', 'Сума': '',
                          'is_weighted': is_w})

        if not mrows:
            continue
        rows.extend(mrows)

        tcols = _agg_cols(df_a)
        # Кінцевий залишок = початковий залишок + нетто-рух за період
        tz    = round(bs + float(df_a['Кількість'].sum()), 4)
        price = prices.get(art)
        ts    = round(tz * price, 2) if price else None

        rows.append({'type': 'subtotal', 'Артикул': art, 'Назва': name, 'Місяць': 'РАЗОМ',
                     **tcols, 'Залишок': tz, 'Ціна': price or '', 'Сума': ts or '',
                     'is_weighted': is_w})
        rows.append({'type': 'spacer'})

        grand['ПрВ']     += tcols['ПрВ'];  grand['Кнк']     += tcols['Кнк']
        grand['ПрИ']     += tcols['ПрИ'];  grand['СпП']     += tcols['СпП']
        # Апс і ПрИ — знакові суми (abs() не застосовується); ПрВ/Кнк/СпП вже abs() з _agg_cols()
        grand['Апс']     += tcols['Апс'];  grand['Залишок'] += tz
        if ts:
            grand['Сума'] += ts

    grand['Сума'] = round(grand['Сума'], 2)
    return rows, grand


def build_summary_rows(ops_df, prices, balance_starts=None):
    """Сумарний звіт — один рядок на артикул, без місяцної розбивки.

    balance_starts: dict {article_id → float} — залишок на початок періоду.
    Залишок = balance_start + SUM(qty за період) = кінцевий залишок.
    """
    if ops_df.empty:
        return [], {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0, 'Залишок': 0, 'Сума': 0.0}

    _bs = balance_starts or {}

    articles = ops_df.groupby('Артикул')['Назва'].first().reset_index()
    rows  = []
    grand = {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0, 'Залишок': 0, 'Сума': 0.0}

    for _, ar in articles.iterrows():
        art, name = ar['Артикул'], ar['Назва']
        df_a  = ops_df[ops_df['Артикул'] == art]
        is_w  = _is_weighted(df_a['Кількість'])
        bs    = float(_bs.get(art, 0.0))
        cols  = _agg_cols(df_a)
        if all(v == 0 for v in cols.values()):
            continue
        # Кінцевий залишок = початковий залишок + нетто-рух за період
        zal   = round(bs + float(df_a['Кількість'].sum()), 4)
        price = prices.get(art)
        suma  = round(zal * price, 2) if price else None

        rows.append({'type': 'summary', 'Артикул': art, 'Назва': name,
                     **cols, 'Залишок': zal, 'Ціна': price or '', 'Сума': suma or '',
                     'is_weighted': is_w})

        grand['ПрВ']     += cols['ПрВ'];  grand['Кнк']     += cols['Кнк']
        grand['ПрИ']     += cols['ПрИ'];  grand['СпП']     += cols['СпП']
        # Апс і ПрИ — знакові суми (abs() не застосовується); ПрВ/Кнк/СпП вже abs() з _agg_cols()
        grand['Апс']     += cols['Апс'];  grand['Залишок'] += zal
        if suma:
            grand['Сума'] += suma

    grand['Сума'] = round(grand['Сума'], 2)
    return rows, grand


def build_document_rows(ops_df, prices, balance_starts=None):
    """
    Звіт «По документах» — хронологічний список операцій на кожен артикул
    з накопичувальним залишком (Running Total).

    balance_starts: dict {article_id → float} — залишок на початок періоду
                    (col E з XLS).  Накопичувальний залишок стартує з цього
                    значення, тому перший рядок показує вже коректний залишок
                    навіть якщо початковий залишок ненульовий.

    Прихід/Розхід визначається по назві операції, а НЕ по знаку qty:
      ПрВ, Ппт(Прихід)               → в колонку Прихід
      Кнк, ПрИ, Ппт(Розхід), СпП → в колонку Розхід
      Апк (корегування)          → qty > 0: Прихід; qty < 0: Розхід

    Піддокумент (subdoc_type/subdoc_code) відображається у тій самій комірці
    «Документ» через символ « → »:
      ПрВ/X016-0337301 - 13.11.25 → Ппт/DP-30954951 - Зпт/X016-0006879
    """
    if ops_df.empty:
        return [], {'Прихід': 0, 'Розхід': 0, 'Залишок': 0}

    _bs = balance_starts or {}

    articles = ops_df.groupby('Артикул')['Назва'].first().reset_index()
    rows  = []
    grand = {'Прихід': 0.0, 'Розхід': 0.0, 'Залишок': 0}

    for _, ar in articles.iterrows():
        art, name = ar['Артикул'], ar['Назва']
        df_a = ops_df[ops_df['Артикул'] == art].copy()
        df_a = df_a.sort_values('Дата', na_position='last').reset_index(drop=True)
        is_w = _is_weighted(df_a['Кількість'])

        # Накопичувальний залишок стартує з початкового залишку артикулу
        running_balance = float(_bs.get(art, 0.0))
        art_pryhid  = 0.0
        art_rozkhid = 0.0

        for _, op in df_a.iterrows():
            qty = float(op['Кількість'])
            op_name = op['Операція']

            # Прихід/Розхід — за назвою операції, а не знаком qty
            pryhid_val, rozkhid_val = _doc_pryhid_rozkhid(op_name, qty)

            running_balance = round(running_balance + qty, 4)

            d = op['Дата']
            date_str = str(d)[:10] if d is not None and str(d) not in ('NaT', 'None', 'nan') else ''

            # Формуємо рядок документа: головний + піддокумент в одній комірці
            doc_main = op.get('Документ', '') or ''
            subdoc   = op.get('Піддокумент', '') or ''
            doc_full = f"{doc_main} → {subdoc}" if subdoc else doc_main

            rows.append({
                'type':        'doc_data',
                'Артикул':     art,
                'Назва':       name,
                'Дата':        date_str,
                'Операція':    op_name,
                'Документ':    doc_full,
                'Прихід':      pryhid_val  if pryhid_val  else '',
                'Розхід':      rozkhid_val if rozkhid_val else '',
                'Кількість':   qty,
                'Залишок':     running_balance,
                'is_weighted': is_w,
            })
            art_pryhid  += pryhid_val
            art_rozkhid += rozkhid_val

        rows.append({'type': 'spacer'})
        grand['Прихід']  += art_pryhid
        grand['Розхід']  += art_rozkhid
        grand['Залишок'] += running_balance

    grand['Прихід']  = round(grand['Прихід'],  2)
    grand['Розхід']  = round(grand['Розхід'],  2)
    return rows, grand
