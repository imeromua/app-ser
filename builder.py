"""
builder.py — побудова рядків для детального, сумарного та документального звітів.
"""

_OP_MAP = {
    'ПрВ (Прихід)':                'ПрВ',
    'Кнк (Продажі)':               'Кнк',
    'ПрИ (Переміщення)':           'ПрИ',
    'СпП (Списання)':              'СпП',
    'Апс (Акт пересорту)':         'Апс',
}

def _agg_cols(df_slice):
    """
    Повертає dict з агрегованими значеннями.
    Беремо абсолютні (додатні) значення для ПрВ, Кнк, СпП та ПрИ!
    Апс залишаємо зі знаком, оскільки це може бути + або -.
    """
    totals = {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0}
    for op, col in _OP_MAP.items():
        mask = df_slice['Операція'] == op
        s = df_slice.loc[mask, 'Кількість'].sum()
        # ТУТ ФІКС: ПрИ тепер теж береться по модулю
        if col in ('ПрВ', 'Кнк', 'СпП', 'ПрИ'):
            totals[col] += abs(s)
        else:
            totals[col] += s
    return {k: int(v) for k, v in totals.items()}

def build_rows(ops_df, prices):
    if ops_df.empty:
        return [], {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0, 'Залишок': 0, 'Сума': 0.0}

    articles = ops_df.groupby('Артикул')['Назва'].first().reset_index()
    months   = sorted(ops_df['Рік-Місяць'].dropna().unique())
    rows     = []
    grand    = {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0, 'Залишок': 0, 'Сума': 0.0}

    for _, ar in articles.iterrows():
        art, name = ar['Артикул'], ar['Назва']
        df_a = ops_df[ops_df['Артикул'] == art]
        mrows = []

        for month in months:
            dm = df_a[df_a['Рік-Місяць'] == month]
            if dm.empty:
                continue
            cols = _agg_cols(dm)
            if all(v == 0 for v in cols.values()):
                continue
            zal = int(dm['Кількість'].sum())
            mrows.append({'type': 'data', 'Артикул': art, 'Назва': name, 'Місяць': month,
                          **cols, 'Залишок': zal, 'Ціна': '', 'Сума': ''})

        if not mrows:
            continue
        rows.extend(mrows)

        tcols = _agg_cols(df_a)
        tz    = int(df_a['Кількість'].sum())
        price = prices.get(art)
        ts    = round(tz * price, 2) if price else None

        rows.append({'type': 'subtotal', 'Артикул': art, 'Назва': name, 'Місяць': 'РАЗОМ',
                     **tcols, 'Залишок': tz, 'Ціна': price or '', 'Сума': ts or ''})
        rows.append({'type': 'spacer'})

        grand['ПрВ']     += tcols['ПрВ'];  grand['Кнк']     += tcols['Кнк']
        grand['ПрИ']     += tcols['ПрИ'];  grand['СпП']     += tcols['СпП']
        grand['Апс']     += tcols['Апс'];  grand['Залишок'] += tz
        if ts:
            grand['Сума'] += ts

    grand['Сума'] = round(grand['Сума'], 2)
    return rows, grand

def build_summary_rows(ops_df, prices):
    if ops_df.empty:
        return [], {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0, 'Залишок': 0, 'Сума': 0.0}

    articles = ops_df.groupby('Артикул')['Назва'].first().reset_index()
    rows  = []
    grand = {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0, 'Залишок': 0, 'Сума': 0.0}

    for _, ar in articles.iterrows():
        art, name = ar['Артикул'], ar['Назва']
        df_a  = ops_df[ops_df['Артикул'] == art]
        cols  = _agg_cols(df_a)
        if all(v == 0 for v in cols.values()):
            continue
        zal   = int(df_a['Кількість'].sum())
        price = prices.get(art)
        suma  = round(zal * price, 2) if price else None

        rows.append({'type': 'summary', 'Артикул': art, 'Назва': name,
                     **cols, 'Залишок': zal, 'Ціна': price or '', 'Сума': suma or ''})

        grand['ПрВ']     += cols['ПрВ'];  grand['Кнк']     += cols['Кнк']
        grand['ПрИ']     += cols['ПрИ'];  grand['СпП']     += cols['СпП']
        grand['Апс']     += cols['Апс'];  grand['Залишок'] += zal
        if suma:
            grand['Сума'] += suma

    grand['Сума'] = round(grand['Сума'], 2)
    return rows, grand

def build_document_rows(ops_df, prices):
    if ops_df.empty:
        return [], {'Прихід': 0, 'Розхід': 0, 'Залишок': 0}

    articles = ops_df.groupby('Артикул')['Назва'].first().reset_index()
    rows  = []
    grand = {'Прихід': 0.0, 'Розхід': 0.0, 'Залишок': 0}

    has_pryhid = 'Прихід' in ops_df.columns
    has_rozkhid = 'Розхід' in ops_df.columns

    for _, ar in articles.iterrows():
        art, name = ar['Артикул'], ar['Назва']
        df_a = ops_df[ops_df['Артикул'] == art].copy()
        df_a = df_a.sort_values('Дата', na_position='last').reset_index(drop=True)

        running_balance = 0
        art_pryhid = 0.0
        art_rozkhid = 0.0

        for _, op in df_a.iterrows():
            qty         = float(op['Кількість'])
            pryhid_val  = float(op['Прихід']) if has_pryhid else max(0.0, qty)
            rozkhid_val = float(op['Розхід']) if has_rozkhid else abs(min(0.0, qty))
            running_balance = round(running_balance + qty, 4)

            d = op['Дата']
            date_str = str(d)[:10] if d is not None and str(d) not in ('NaT', 'None', 'nan') else ''

            rows.append({
                'type':      'doc_data',
                'Артикул':   art,
                'Назва':     name,
                'Дата':      date_str,
                'Операція':  op['Операція'],
                'Документ':  op.get('Документ', ''),
                'Прихід':    pryhid_val if pryhid_val else '',
                'Розхід':    rozkhid_val if rozkhid_val else '',
                'Кількість': qty,
                'Залишок':   running_balance,
            })
            art_pryhid  += pryhid_val
            art_rozkhid += rozkhid_val

        rows.append({'type': 'spacer'})
        grand['Прихід']  += art_pryhid
        grand['Розхід']  += art_rozkhid
        grand['Залишок'] += running_balance

    grand['Прихід']  = round(grand['Прихід'], 2)
    grand['Розхід']  = round(grand['Розхід'], 2)
    return rows, grand