"""
builder.py — побудова рядків для детального та сумарного звітів.
"""


def build_rows(ops_df, prices):
    """Детальний звіт — розбивка по місяцях для кожного артикулу."""
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
            prv = int(dm[dm['Операція'] == 'ПрВ']['Кількість'].sum())
            knk = int(dm[dm['Операція'] == 'Кнк']['Кількість'].sum())
            pri = int(dm[dm['Операція'] == 'ПрИ']['Кількість'].sum())
            spp = int(dm[dm['Операція'] == 'СпП']['Кількість'].sum())
            aps = int(dm[dm['Операція'] == 'Апс']['Кількість'].sum())
            # spp (СпП) values may be negative in source data; adding them reduces the balance
            zal = (prv + spp + pri) - knk - aps
            if all(v == 0 for v in [prv, knk, pri, spp, aps]):
                continue
            mrows.append({'type': 'data', 'Артикул': art, 'Назва': name, 'Місяць': month,
                          'ПрВ': prv, 'Кнк': knk, 'ПрИ': abs(pri), 'СпП': abs(spp),
                          'Апс': aps, 'Залишок': zal, 'Ціна': '', 'Сума': '',
                          '_pri_s': pri, '_spp_s': spp})

        if not mrows:
            continue
        rows.extend(mrows)

        tp  = sum(r['ПрВ']    for r in mrows)
        tk  = sum(r['Кнк']    for r in mrows)
        tpi = sum(r['_pri_s'] for r in mrows)
        tsp = sum(r['_spp_s'] for r in mrows)
        ta  = sum(r['Апс']    for r in mrows)
        tz  = (tp + tsp + tpi) - tk - ta
        price = prices.get(art)
        ts    = round(tz * price, 2) if price else None

        rows.append({'type': 'subtotal', 'Артикул': art, 'Назва': name, 'Місяць': 'РАЗОМ',
                     'ПрВ': tp, 'Кнк': tk, 'ПрИ': abs(tpi), 'СпП': abs(tsp), 'Апс': ta,
                     'Залишок': tz, 'Ціна': price or '', 'Сума': ts or ''})
        rows.append({'type': 'spacer'})

        grand['ПрВ']     += tp;  grand['Кнк']     += tk
        grand['ПрИ']     += abs(tpi); grand['СпП'] += abs(tsp)
        grand['Апс']     += ta;  grand['Залишок']  += tz
        if ts:
            grand['Сума'] += ts

    grand['Сума'] = round(grand['Сума'], 2)
    return rows, grand


def build_summary_rows(ops_df, prices):
    """Сумарний звіт — один рядок на артикул, без місяцної розбивки."""
    if ops_df.empty:
        return [], {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0, 'Залишок': 0, 'Сума': 0.0}

    articles = ops_df.groupby('Артикул')['Назва'].first().reset_index()
    rows = []
    grand = {'ПрВ': 0, 'Кнк': 0, 'ПрИ': 0, 'СпП': 0, 'Апс': 0, 'Залишок': 0, 'Сума': 0.0}

    for _, ar in articles.iterrows():
        art, name = ar['Артикул'], ar['Назва']
        df_a = ops_df[ops_df['Артикул'] == art]

        prv = int(df_a[df_a['Операція'] == 'ПрВ']['Кількість'].sum())
        knk = int(df_a[df_a['Операція'] == 'Кнк']['Кількість'].sum())
        pri = int(df_a[df_a['Операція'] == 'ПрИ']['Кількість'].sum())
        spp = int(df_a[df_a['Операція'] == 'СпП']['Кількість'].sum())
        aps = int(df_a[df_a['Операція'] == 'Апс']['Кількість'].sum())
        # spp (СпП) values may be negative in source data; adding them reduces the balance
        zal = (prv + spp + pri) - knk - aps
        price = prices.get(art)
        suma = round(zal * price, 2) if price else None

        if all(v == 0 for v in [prv, knk, pri, spp, aps]):
            continue

        rows.append({'type': 'summary', 'Артикул': art, 'Назва': name,
                     'ПрВ': prv, 'Кнк': knk, 'ПрИ': abs(pri),
                     'СпП': abs(spp), 'Апс': aps, 'Залишок': zal,
                     'Ціна': price or '', 'Сума': suma or ''})

        grand['ПрВ']     += prv
        grand['Кнк']     += knk
        grand['ПрИ']     += abs(pri)
        grand['СпП']     += abs(spp)
        grand['Апс']     += aps
        grand['Залишок'] += zal
        if suma:
            grand['Сума'] += suma

    grand['Сума'] = round(grand['Сума'], 2)
    return rows, grand
