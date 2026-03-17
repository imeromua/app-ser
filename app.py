#!/usr/bin/env python3
"""
Рух товарів — веб-аналізатор
Запуск: python app.py  →  http://localhost:5000
"""

from flask import Flask, request, render_template_string, send_file
import pandas as pd
import re, io
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

# ─────────────────────────────────────────
DOC_PREFIXES = ('ПрВ', 'Кнк', 'СпП', 'СпО', 'ПрИ', 'Апс')

def is_article_code(val):
    s = str(val).strip()
    return s.isdigit() and len(s) >= 5

def parse_xls(buf):
    df = pd.read_excel(buf, sheet_name=0, header=None)

    def cell(r, c):
        try:
            v = df.iloc[r, c]
            return str(v).strip() if pd.notna(v) else ''
        except:
            return ''

    header = {
        'title':     cell(0, 0),
        'shop':      cell(1, 3),
        'period':    cell(1, 10),
        'warehouse': cell(3, 3),
    }

    data, i = [], 15
    cur_art, cur_name = None, ''

    while i < len(df):
        row    = df.iloc[i]
        marker = row[0]
        col1   = str(row[1]).strip() if pd.notna(row[1]) else ''

        if marker == '+' and is_article_code(col1):
            cur_art  = col1
            cur_name = str(row[2]).strip() if pd.notna(row[2]) else ''
            i += 1; continue

        if marker == '-':
            if is_article_code(col1):
                cur_art  = col1
                cur_name = str(row[2]).strip() if pd.notna(row[2]) else cur_name
                i += 1; continue

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
                    except: pass
        i += 1

    prices = {}
    for i2 in range(15, len(df)):
        r2 = df.iloc[i2]
        if r2[0] == '+' and is_article_code(str(r2[1]).strip()):
            try:
                p = float(r2[11])
                if p > 0: prices[str(r2[1]).strip()] = p
            except: pass

    return header, pd.DataFrame(data), prices


def build_rows(ops_df, prices):
    if ops_df.empty:
        return [], {'ПрВ':0,'Кнк':0,'ПрИ':0,'СпП':0,'Апс':0,'Залишок':0,'Сума':0.0}

    articles = ops_df.groupby('Артикул')['Назва'].first().reset_index()
    months   = sorted(ops_df['Рік-Місяць'].dropna().unique())
    rows     = []
    grand    = {'ПрВ':0,'Кнк':0,'ПрИ':0,'СпП':0,'Апс':0,'Залишок':0,'Сума':0.0}

    for _, ar in articles.iterrows():
        art, name = ar['Артикул'], ar['Назва']
        df_a = ops_df[ops_df['Артикул'] == art]
        mrows = []

        for month in months:
            dm = df_a[df_a['Рік-Місяць'] == month]
            if dm.empty: continue
            prv = int(dm[dm['Операція']=='ПрВ']['Кількість'].sum())
            knk = int(dm[dm['Операція']=='Кнк']['Кількість'].sum())
            pri = int(dm[dm['Операція']=='ПрИ']['Кількість'].sum())
            spp = int(dm[dm['Операція']=='СпП']['Кількість'].sum())
            aps = int(dm[dm['Операція']=='Апс']['Кількість'].sum())
            zal = (prv + spp + pri) - knk - aps
            if all(v == 0 for v in [prv, knk, pri, spp, aps]): continue
            mrows.append({'type':'data','Артикул':art,'Назва':name,'Місяць':month,
                          'ПрВ':prv,'Кнк':knk,'ПрИ':abs(pri),'СпП':abs(spp),
                          'Апс':aps,'Залишок':zal,'Ціна':'','Сума':'',
                          '_pri_s':pri,'_spp_s':spp})

        if not mrows: continue
        rows.extend(mrows)

        tp  = sum(r['ПрВ']   for r in mrows)
        tk  = sum(r['Кнк']   for r in mrows)
        tpi = sum(r['_pri_s'] for r in mrows)
        tsp = sum(r['_spp_s'] for r in mrows)
        ta  = sum(r['Апс']   for r in mrows)
        tz  = (tp + tsp + tpi) - tk - ta
        price = prices.get(art)
        ts    = round(tz * price, 2) if price else None

        rows.append({'type':'subtotal','Артикул':art,'Назва':name,'Місяць':'РАЗОМ',
                     'ПрВ':tp,'Кнк':tk,'ПрИ':abs(tpi),'СпП':abs(tsp),'Апс':ta,
                     'Залишок':tz,'Ціна':price or '','Сума':ts or ''})
        rows.append({'type':'spacer'})

        grand['ПрВ']     += tp;  grand['Кнк']     += tk
        grand['ПрИ']     += abs(tpi); grand['СпП'] += abs(tsp)
        grand['Апс']     += ta;  grand['Залишок']  += tz
        if ts: grand['Сума'] += ts

    grand['Сума'] = round(grand['Сума'], 2)
    return rows, grand


def export_excel(header, rows, grand):
    out  = io.BytesIO()
    cols = ['Артикул','Назва','Місяць','ПрВ','Кнк','ПрИ','СпП','Апс','Залишок','Ціна','Сума']
    hfill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
    sfill = PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid')
    gfill = PatternFill(start_color='1F3864', end_color='1F3864', fill_type='solid')
    hfont = Font(color='FFFFFF', bold=True)
    sfont = Font(bold=True, color='1F3864')
    gfont = Font(color='FFFFFF', bold=True)

    with pd.ExcelWriter(out, engine='openpyxl') as writer:
        pd.DataFrame([[]]).to_excel(writer, index=False, header=False,
                                    sheet_name='Рух товарів', startrow=0)
        wb = writer.book
        ws = writer.sheets['Рух товарів']

        ws['A1'].value = header.get('title','')
        ws['A1'].font  = Font(bold=True, size=14, color='C0392B')
        ws['A2'].value = 'Магазин:';  ws['D2'].value = header.get('shop','')
        ws['K2'].value = header.get('period','')
        ws['A4'].value = 'Склад:';    ws['D4'].value = header.get('warehouse','')

        HR = 6
        for ci, cn in enumerate(cols, 1):
            c = ws.cell(row=HR, column=ci, value=cn)
            c.fill = hfill; c.font = hfont
            c.alignment = Alignment(horizontal='center')

        dr = HR + 1
        for row in rows:
            rt = row.get('type')
            if rt == 'spacer':
                dr += 1; continue
            for ci, cn in enumerate(cols, 1):
                val  = row.get(cn, '')
                cell = ws.cell(row=dr, column=ci)
                if val == '' or val is None:
                    cell.value = None
                elif isinstance(val, str):
                    cell.value = val
                else:
                    try:
                        cell.value = float(val) if cn in ('Ціна','Сума') else int(val)
                        if cn == 'Сума': cell.number_format = '#,##0.00'
                        if cn == 'Ціна': cell.number_format = '0.00'
                    except:
                        cell.value = val
                if rt == 'subtotal':
                    cell.fill = sfill; cell.font = sfont
                if ci > 2:
                    cell.alignment = Alignment(horizontal='right')
            dr += 1

        for ci, cn in enumerate(cols, 1):
            cell = ws.cell(row=dr+1, column=ci)
            if cn == 'Назва': cell.value = 'ЗАГАЛЬНИЙ ПІДСУМОК'
            elif cn in grand and grand[cn]:
                try:
                    cell.value = float(grand[cn]) if cn in ('Сума',) else int(grand[cn])
                    if cn == 'Сума': cell.number_format = '#,##0.00'
                except: pass
            cell.fill = gfill; cell.font = gfont
            if ci > 2: cell.alignment = Alignment(horizontal='right')

        for i, w in enumerate([12,42,10,7,7,7,7,7,10,10,13], 1):
            ws.column_dimensions[get_column_letter(i)].width = w
        ws.freeze_panes = f'A{HR+1}'

    out.seek(0)
    return out


# ─────────────────────────────────────────
_cache = {}

INDEX_HTML = """<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Рух товарів — аналіз</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
  <style>
    body{background:#f0f4f8}
    .upload-area{border:2.5px dashed #4472C4;border-radius:16px;padding:60px 40px;text-align:center;
      cursor:pointer;transition:.25s;background:#fff}
    .upload-area:hover,.upload-area.drag{background:#e8effc;border-color:#1F3864}
    .upload-icon{font-size:56px}
    .card-header{background:#1F3864;color:#fff;font-weight:700}
  </style>
</head>
<body>
<div class="container py-5">
  <div class="text-center mb-4">
    <h2 class="fw-bold text-danger">Рух товарів кількісний</h2>
    <p class="text-muted">Завантажте XLS-файл звіту EPI — отримайте зведену таблицю з підсумками та сумами залишків</p>
  </div>
  <div class="row justify-content-center">
    <div class="col-md-6">
      <div class="card shadow-sm">
        <div class="card-header">📂 Завантаження файлу</div>
        <div class="card-body">
          <form id="form" action="/upload" method="post" enctype="multipart/form-data">
            <div class="upload-area" id="drop" onclick="document.getElementById('fi').click()">
              <div class="upload-icon">📊</div>
              <p class="fs-5 mt-2 mb-1 fw-semibold">Перетягніть XLS-файл сюди</p>
              <p class="text-muted small">або натисніть щоб обрати</p>
              <p id="fn" class="text-success fw-semibold mt-2"></p>
            </div>
            <input type="file" id="fi" name="file" accept=".xls,.xlsx" class="d-none">
            <div id="spin" class="mt-3 text-center d-none">
              <div class="spinner-border text-primary me-2" role="status"></div>
              <span>Обробляємо файл...</span>
            </div>
            <button type="submit" id="btn" class="btn btn-primary w-100 mt-3" disabled>🔍 Аналізувати</button>
          </form>
          {% if error %}<div class="alert alert-danger mt-3">{{ error }}</div>{% endif %}
        </div>
      </div>
    </div>
  </div>
</div>
<script>
const drop=document.getElementById('drop'),fi=document.getElementById('fi'),
      btn=document.getElementById('btn'),fn=document.getElementById('fn'),
      form=document.getElementById('form'),spin=document.getElementById('spin');
fi.addEventListener('change',()=>{if(fi.files.length){fn.textContent='\u2713 '+fi.files[0].name;btn.disabled=false;}});
['dragenter','dragover'].forEach(e=>drop.addEventListener(e,ev=>{ev.preventDefault();drop.classList.add('drag');}));
['dragleave','drop'].forEach(e=>drop.addEventListener(e,ev=>{ev.preventDefault();drop.classList.remove('drag');}));
drop.addEventListener('drop',ev=>{const f=ev.dataTransfer.files[0];if(f){const dt=new DataTransfer();
  dt.items.add(f);fi.files=dt.files;fn.textContent='\u2713 '+f.name;btn.disabled=false;}});
form.addEventListener('submit',()=>{spin.classList.remove('d-none');btn.disabled=true;});
</script>
</body></html>"""

RESULT_HTML = """<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Рух товарів — результат</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
  <style>
    body{background:#f0f4f8;font-size:13px}
    .report-header{background:#fff;border-left:5px solid #C0392B;border-radius:8px;padding:14px 22px;margin-bottom:18px}
    .report-title{color:#C0392B;font-size:1.35rem;font-weight:700;text-decoration:underline}
    .report-meta span{margin-right:20px;color:#555}
    .report-meta strong{color:#1F3864}
    thead th{background:#1F3864!important;color:#fff!important;text-align:center;
      border:1px solid #4472C4!important;white-space:nowrap;padding:7px 10px}
    tr.subtotal td{background:#D9E1F2!important;font-weight:700;color:#1F3864}
    tr.grand td{background:#1F3864!important;color:#fff!important;font-weight:700}
    tr.spacer{height:5px;background:#f0f4f8!important}
    td.num{text-align:right;padding-right:10px!important}
    td.ctr{text-align:center}
    .stat-card{border-radius:12px;padding:12px 16px;color:#fff;text-align:center}
    .stat-val{font-size:1.6rem;font-weight:800}
    .stat-lbl{font-size:.75rem;opacity:.85}
    .toolbar{background:#fff;border-radius:8px;padding:10px 14px;margin-bottom:14px}
    table{border-collapse:collapse!important}
    td,th{border:1px solid #dee2e6!important}
    tr.data-row:hover td{background:#f8f9fa!important}
  </style>
</head>
<body>
<div class="container-fluid py-3 px-4">

  <div class="report-header shadow-sm">
    <div class="report-title">{{ header.title }}</div>
    <div class="report-meta mt-1">
      <span><strong>Магазин:</strong> {{ header.shop }}</span>
      <span><strong>Склад:</strong> {{ header.warehouse }}</span>
      <span><strong>Період:</strong> {{ header.period }}</span>
    </div>
  </div>

  <div class="row g-3 mb-3">
    {% set cards = [
      ('ПрВ (прихід)', grand.ПрВ, '#1F3864'),
      ('Кнк (продажі)', grand.Кнк, '#c0392b'),
      ('ПрИ (переміщ.)', grand.ПрИ, '#7f8c8d'),
      ('СпП (списання)', grand.СпП, '#e67e22'),
      ('Залишок, шт', grand.Залишок, '#27ae60'),
      ('Сума залишків, грн', grand.Сума|int, '#8e44ad'),
    ] %}
    {% for lbl,val,clr in cards %}
    <div class="col-6 col-md-2">
      <div class="stat-card shadow-sm" style="background:{{clr}}">
        <div class="stat-val">{{ "{:,}".format(val) }}</div>
        <div class="stat-lbl">{{ lbl }}</div>
      </div>
    </div>
    {% endfor %}
  </div>

  <div class="toolbar d-flex gap-2 align-items-center flex-wrap shadow-sm">
    <a href="/" class="btn btn-outline-secondary btn-sm">← Новий файл</a>
    <a href="/download" class="btn btn-success btn-sm">⬇ Завантажити Excel</a>
    <span class="text-muted small ms-2">{{ row_count }} рядків | {{ art_count }} артикулів</span>
    <input id="srch" type="text" class="form-control form-control-sm ms-auto"
           style="max-width:260px" placeholder="🔍 Пошук по таблиці...">
  </div>

  <div class="card shadow-sm">
    <div class="card-body p-0">
      <table class="table table-sm table-bordered mb-0 w-100" id="tbl">
        <thead>
          <tr>
            <th>Артикул</th><th>Назва товару</th><th>Місяць</th>
            <th>ПрВ</th><th>Кнк</th><th>ПрИ</th><th>СпП</th><th>Апс</th>
            <th>Залишок</th><th>Ціна</th><th>Сума, грн</th>
          </tr>
        </thead>
        <tbody>
        {% for row in rows %}
          {% if row.type == 'spacer' %}
            <tr class="spacer"><td colspan="11"></td></tr>
          {% elif row.type == 'subtotal' %}
            <tr class="subtotal">
              <td>{{ row.Артикул }}</td>
              <td>▶ {{ row.Назва[:52] }}</td>
              <td class="ctr fw-bold">{{ row.Місяць }}</td>
              <td class="num">{{ row.ПрВ }}</td>
              <td class="num">{{ row.Кнк }}</td>
              <td class="num">{{ row.ПрИ if row.ПрИ else '' }}</td>
              <td class="num">{{ row.СпП if row.СпП else '' }}</td>
              <td class="num">{{ row.Апс if row.Апс else '' }}</td>
              <td class="num fw-bold">{{ row.Залишок }}</td>
              <td class="num">{{ "%.2f"|format(row.Ціна) if row.Ціна != '' else '' }}</td>
              <td class="num">{{ "{:,.2f}".format(row.Сума) if row.Сума != '' else '' }}</td>
            </tr>
          {% else %}
            <tr class="data-row">
              <td>{{ row.Артикул }}</td>
              <td>{{ row.Назва[:55] }}</td>
              <td class="ctr">{{ row.Місяць }}</td>
              <td class="num">{{ row.ПрВ if row.ПрВ else '' }}</td>
              <td class="num">{{ row.Кнк if row.Кнк else '' }}</td>
              <td class="num">{{ row.ПрИ if row.ПрИ else '' }}</td>
              <td class="num">{{ row.СпП if row.СпП else '' }}</td>
              <td class="num">{{ row.Апс if row.Апс else '' }}</td>
              <td class="num">{{ row.Залишок }}</td>
              <td></td><td></td>
            </tr>
          {% endif %}
        {% endfor %}
        </tbody>
        <tfoot>
          <tr class="grand">
            <td colspan="3" class="ps-2 fw-bold">ЗАГАЛЬНИЙ ПІДСУМОК</td>
            <td class="num">{{ grand.ПрВ }}</td>
            <td class="num">{{ grand.Кнк }}</td>
            <td class="num">{{ grand.ПрИ }}</td>
            <td class="num">{{ grand.СпП }}</td>
            <td class="num">{{ grand.Апс }}</td>
            <td class="num fw-bold">{{ grand.Залишок }}</td>
            <td></td>
            <td class="num fw-bold">{{ "{:,.2f}".format(grand.Сума) }}</td>
          </tr>
        </tfoot>
      </table>
    </div>
  </div>
</div>
<script>
document.getElementById('srch').addEventListener('input',function(){
  const q=this.value.toLowerCase();
  document.querySelectorAll('#tbl tbody tr').forEach(tr=>{
    if(tr.classList.contains('spacer')) return;
    tr.style.display=tr.textContent.toLowerCase().includes(q)?'':'none';
  });
});
</script>
</body></html>"""


@app.route('/')
def index():
    return render_template_string(INDEX_HTML, error=None)

@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return render_template_string(INDEX_HTML, error='Файл не знайдено')
    f = request.files['file']
    if not f.filename:
        return render_template_string(INDEX_HTML, error='Файл не вибрано')
    try:
        buf = io.BytesIO(f.read())
        header, ops_df, prices = parse_xls(buf)
        rows, grand = build_rows(ops_df, prices)
        _cache['last'] = (header, rows, grand)
        art_count = len(set(r['Артикул'] for r in rows if r.get('type') == 'subtotal'))
        row_count = sum(1 for r in rows if r.get('type') == 'data')
        return render_template_string(RESULT_HTML,
            header=header, rows=rows, grand=grand,
            art_count=art_count, row_count=row_count)
    except Exception as e:
        return render_template_string(INDEX_HTML, error=f'Помилка: {e}')

@app.route('/download')
def download():
    if 'last' not in _cache:
        return 'Немає даних', 400
    header, rows, grand = _cache['last']
    buf = export_excel(header, rows, grand)
    return send_file(buf, as_attachment=True,
                     download_name='ruh_tovariv.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
