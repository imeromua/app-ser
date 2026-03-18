#!/usr/bin/env python3
"""
Рух товарів — веб-аналізатор
Запуск: python app.py  →  http://localhost:5000
"""

import io
import logging
import os
import secrets
import threading
import uuid

import openpyxl
from openpyxl.styles import Font as XlFont, Alignment as XlAlign, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

from celery.result import AsyncResult
from flask import Flask, request, render_template, send_file, session, jsonify, Response as FlaskResponse
from werkzeug.utils import secure_filename
import pandas as pd

from categories import detect_category
from session_store import save_session_data, load_session_data, cleanup_old_sessions
from parser import parse_xls
from builder import build_rows, build_summary_rows, build_document_rows
from exporter import export_excel
from tasks import celery, generate_pdf_task  # noqa: F401 — celery app must be imported

logging.basicConfig(level=logging.WARNING)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024
app.secret_key = os.environ.get('SECRET_KEY') or secrets.token_hex(32)


@app.route('/')
def index():
    return render_template('index.html', error=None)


@app.route('/upload', methods=['POST'])
def upload():
    files = request.files.getlist('files')
    if not files or all(not f.filename for f in files):
        return render_template('index.html', error='Файл не знайдено')
    try:
        report_type = request.form.get('report_type', 'detail')
        all_ops, all_prices = [], {}
        first_header = None

        for f in files:
            if not f.filename:
                continue
            buf = io.BytesIO(f.read())
            hdr, ops_df, prices = parse_xls(buf)
            if first_header is None:
                first_header = hdr
            all_ops.append(ops_df)
            all_prices.update(prices)

        combined_df = pd.concat(all_ops, ignore_index=True, sort=False) if all_ops else pd.DataFrame()
        header = first_header or {}

        if report_type == 'summary':
            rows, grand = build_summary_rows(combined_df, all_prices)
        elif report_type == 'document':
            rows, grand = build_document_rows(combined_df, all_prices)
        else:
            rows, grand = build_rows(combined_df, all_prices)

        if report_type == 'document':
            all_names = [r['Назва'] for r in rows if r.get('type') == 'doc_data']
            art_count = len(set(r['Артикул'] for r in rows if r.get('type') == 'doc_data'))
            row_count = sum(1 for r in rows if r.get('type') == 'doc_data')
        else:
            all_names = [r['Назва'] for r in rows if r.get('type') in ('data', 'summary')]
            art_count = len(set(
                r['Артикул'] for r in rows
                if r.get('type') in ('subtotal', 'summary')
            ))
            row_count = sum(1 for r in rows if r.get('type') in ('data', 'summary'))

        category = detect_category(all_names)
        safe_category = category.replace('/', '-').replace(' ', '_')

        if report_type == 'summary':
            download_name = f"{safe_category}_сумарний_звіт.xlsx"
        elif report_type == 'document':
            download_name = f"{safe_category}_по_документах.xlsx"
        else:
            download_name = f"{safe_category}_детальний_звіт.xlsx"

        # Store data in a server-side temp file; only keep UUID in cookie
        session_id = save_session_data({
            'header': header,
            'rows': rows,
            'grand': grand,
            'filename': download_name,
            'category': category,
            'report_type': report_type,
        })
        session['sid'] = session_id
        threading.Thread(target=cleanup_old_sessions, daemon=True).start()

        return render_template('result.html',
                               header=header, rows=rows, grand=grand,
                               art_count=art_count, row_count=row_count,
                               category=category, report_type=report_type)
    except Exception as e:
        logging.exception('Error processing uploaded file')
        return render_template('index.html', error=f'Помилка обробки файлу: {e}')


@app.route('/download')
def download():
    sid = session.get('sid')
    data = load_session_data(sid)
    if not data:
        return 'Немає даних або сесія застаріла', 400
    header        = data['header']
    rows          = data['rows']
    grand         = data['grand']
    download_name = data.get('filename', 'ruh_tovariv_звіт.xlsx')
    report_type   = data.get('report_type', 'detail')
    buf = export_excel(header, rows, grand, report_type=report_type)
    return send_file(buf, as_attachment=True,
                     download_name=download_name,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/download_inventory')
def download_inventory():
    sid = session.get('sid')
    data = load_session_data(sid)
    if not data:
        return 'Немає даних або сесія застаріла', 400

    rows = data.get('rows', [])
    report_type = data.get('report_type', 'detail')
    category = data.get('category', 'товари')
    hdr = data.get('header', {})
    safe_category = category.replace('/', '-').replace(' ', '_')

    # Select the rows that represent one summary entry per article
    if report_type == 'document':
        # Last doc_data row per article carries the final running balance
        article_map: dict = {}
        for r in rows:
            if r.get('type') == 'doc_data':
                article_map[r['Артикул']] = r
        inv_rows = list(article_map.values())
    else:
        # subtotal rows (detail) and summary rows (summary) are already one per article
        inv_rows = [r for r in rows if r.get('type') in ('subtotal', 'summary')]

    if not inv_rows:
        return 'Немає даних для відомості інвентаризації', 400

    # Sort alphabetically by name, case-insensitive
    inv_rows.sort(key=lambda r: r.get('Назва', '').lower())

    # ── Build xlsx ────────────────────────────────────────────────────────────
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Інвентаризація'

    NUM_COLS = 9
    col_widths = [5, 12, 42, 14, 10, 14, 16, 30, 14]
    for ci, w in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w

    # Styles
    title_font   = XlFont(bold=True, underline='single', color='1F3864', size=14)
    label_font   = XlFont(bold=True)
    value_font   = XlFont(color='1F3864')
    hdr_fill     = PatternFill(start_color='9DC3E6', end_color='9DC3E6', fill_type='solid')
    hdr_font     = XlFont(bold=True)
    hdr_align    = XlAlign(horizontal='center', vertical='center', wrap_text=True)
    thin         = Side(style='thin')
    cell_border  = Border(left=thin, right=thin, top=thin, bottom=thin)
    bot_border   = Border(bottom=thin)
    center_align = XlAlign(horizontal='center', vertical='center')
    right_align  = XlAlign(horizontal='right', vertical='center')

    # ── Row 1: Title ──────────────────────────────────────────────────────────
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=NUM_COLS)
    tc = ws.cell(row=1, column=1, value=f'Відомість інвентаризації — {category}')
    tc.font = title_font
    tc.alignment = XlAlign(horizontal='left', vertical='center')
    ws.row_dimensions[1].height = 22

    # ── Row 2: Маркет ─────────────────────────────────────────────────────────
    ws.cell(row=2, column=1, value='Маркет:').font = label_font
    c = ws.cell(row=2, column=2, value=hdr.get('shop', ''))
    c.font = value_font
    ws.merge_cells(start_row=2, start_column=2, end_row=2, end_column=NUM_COLS)

    # ── Row 3: Єрархія ────────────────────────────────────────────────────────
    ws.cell(row=3, column=1, value='Єрархія:').font = label_font
    c = ws.cell(row=3, column=2, value=hdr.get('warehouse', ''))
    c.font = value_font
    ws.merge_cells(start_row=3, start_column=2, end_row=3, end_column=NUM_COLS)

    # ── Row 4: Примітки ───────────────────────────────────────────────────────
    ws.cell(row=4, column=1, value='Примітки:').font = label_font
    c = ws.cell(row=4, column=2, value=f'Позапланова інв_{category}')
    c.font = value_font
    ws.merge_cells(start_row=4, start_column=2, end_row=4, end_column=NUM_COLS)

    # ── Row 5: Spacer ─────────────────────────────────────────────────────────
    ws.row_dimensions[5].height = 8

    # ── Row 6: Table header ───────────────────────────────────────────────────
    HEADER_ROW = 6
    col_headers = [
        '№', 'Артикул', 'Назва',
        'Ціна\nреалізації', 'Од.\nвим.', 'База\n(залишок)',
        'Фактичні\nзалишки', 'Примітки', 'Час\nінвентаризації',
    ]
    for ci, h in enumerate(col_headers, 1):
        cell = ws.cell(row=HEADER_ROW, column=ci, value=h)
        cell.font = hdr_font
        cell.fill = hdr_fill
        cell.alignment = hdr_align
        cell.border = cell_border
    ws.row_dimensions[HEADER_ROW].height = 42

    # ── Data rows ─────────────────────────────────────────────────────────────
    DATA_START = HEADER_ROW + 1
    for i, r in enumerate(inv_rows, 1):
        dr = DATA_START + i - 1

        c = ws.cell(row=dr, column=1, value=i)
        c.border = cell_border
        c.alignment = center_align

        c = ws.cell(row=dr, column=2, value=r.get('Артикул', ''))
        c.border = cell_border
        c.alignment = center_align

        c = ws.cell(row=dr, column=3, value=r.get('Назва', ''))
        c.border = cell_border

        # Ціна реалізації
        price = r.get('Ціна', '')
        c = ws.cell(row=dr, column=4)
        c.border = cell_border
        c.alignment = right_align
        if price != '' and price is not None:
            try:
                c.value = round(float(price), 2)
                c.number_format = '#,##0.00'
            except (ValueError, TypeError):
                c.value = price

        # Од. вим. — left empty
        ws.cell(row=dr, column=5).border = cell_border

        # База (залишок)
        zal = r.get('Залишок', '')
        c = ws.cell(row=dr, column=6)
        c.border = cell_border
        c.alignment = right_align
        if zal != '' and zal is not None:
            try:
                fval = float(zal)
                if fval.is_integer():
                    c.value = int(fval)
                else:
                    c.value = round(fval, 2)
                    c.number_format = '0.##'
            except (ValueError, TypeError):
                c.value = zal

        # Фактичні залишки — empty for manual entry
        ws.cell(row=dr, column=7).border = cell_border
        # Примітки — empty for manual entry
        ws.cell(row=dr, column=8).border = cell_border
        # Час інвентаризації — empty for manual entry
        ws.cell(row=dr, column=9).border = cell_border

    # ── Footer ────────────────────────────────────────────────────────────────
    last_data_row = DATA_START + len(inv_rows) - 1
    fr = last_data_row + 2  # footer start row

    ws.merge_cells(start_row=fr, start_column=1, end_row=fr, end_column=NUM_COLS)
    ws.cell(row=fr, column=1, value='Особи, які проводили перерахунок:')
    ws.row_dimensions[fr].height = 22

    # Two signature blocks (ПІП / підпис)
    for sig_idx in range(2):
        line_row  = fr + 2 + sig_idx * 4
        label_row = line_row + 1

        # Underline for name (ПІП)
        for col_idx in range(4, 7):
            ws.cell(row=line_row, column=col_idx).border = bot_border
        ws.merge_cells(start_row=label_row, start_column=4, end_row=label_row, end_column=6)
        pip = ws.cell(row=label_row, column=4, value='(ПІП)')
        pip.alignment = center_align

        # Underline for signature (підпис)
        for col_idx in range(8, 10):
            ws.cell(row=line_row, column=col_idx).border = bot_border
        ws.merge_cells(start_row=label_row, start_column=8, end_row=label_row, end_column=9)
        sign = ws.cell(row=label_row, column=8, value='(підпис)')
        sign.alignment = center_align

    nachal_row = fr + 2 + 2 * 4 + 1
    ws.merge_cells(start_row=nachal_row, start_column=1, end_row=nachal_row, end_column=NUM_COLS)
    ws.cell(row=nachal_row, column=1,
            value='Начальник відділу  ___________________________________')
    ws.row_dimensions[nachal_row].height = 22

    date_row = nachal_row + 2
    ws.merge_cells(start_row=date_row, start_column=1, end_row=date_row, end_column=NUM_COLS)
    ws.cell(row=date_row, column=1,
            value='Дата проведення  ________________________  час з  ________________  по  ________________')
    ws.row_dimensions[date_row].height = 22

    # ── Output ────────────────────────────────────────────────────────────────
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)

    filename = f'{safe_category}_інвентаризація.xlsx'
    return send_file(out, as_attachment=True,
                     download_name=filename,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/download_pdf')
def download_pdf():
    # DEPRECATED: маршрут видалено. Використовуйте асинхронний API:
    # POST /export/pdf/start → GET /export/pdf/status/<task_id> → GET /export/pdf/result/<task_id>
    return (
        'Цей маршрут більше не підтримується. '
        'Використовуйте /export/pdf/start → /export/pdf/status → /export/pdf/result.',
        410,
    )


# ── Async PDF export via Celery ──────────────────────────────────────────────

@app.route('/export/pdf/start', methods=['POST'])
def export_pdf_start():
    """Запускає фонову задачу генерації PDF. Повертає {"task_id": "..."}."""
    sid = session.get('sid')
    data = load_session_data(sid)
    if not data:
        return jsonify({'error': 'Немає даних або сесія застаріла'}), 400

    html_string = render_template(
        'report_pdf.html',
        header=data['header'],
        rows=data['rows'],
        grand=data['grand'],
        report_type=data.get('report_type', 'detail'),
        category=data.get('category', ''),
    )
    pdf_name = data.get('filename', 'звіт').replace('.xlsx', '.pdf')

    task = generate_pdf_task.delay(html_string, pdf_name)
    return jsonify({'task_id': task.id})


@app.route('/export/pdf/status/<task_id>')
def export_pdf_status(task_id):
    """Повертає стан задачі: pending / ready / error."""
    try:
        uuid.UUID(task_id)
    except ValueError:
        return jsonify({'status': 'error', 'error': 'Невалідний task_id'}), 400

    result = AsyncResult(task_id, app=celery)
    state = result.state

    if state in ('PENDING', 'STARTED'):
        return jsonify({'status': 'pending'})
    if state == 'SUCCESS':
        return jsonify({'status': 'ready'})
    # FAILURE або інший термінальний стан
    error_msg = str(result.result) if result.result else state
    return jsonify({'status': 'error', 'error': error_msg})


@app.route('/export/pdf/result/<task_id>')
def export_pdf_result(task_id):
    """Повертає готовий PDF-файл, якщо задача завершена."""
    try:
        uuid.UUID(task_id)
    except ValueError:
        return jsonify({'error': 'Невалідний task_id'}), 400

    result = AsyncResult(task_id, app=celery)
    if result.state != 'SUCCESS':
        return jsonify({'status': 'pending'}), 202

    task_result = result.result
    if not task_result or not isinstance(task_result, dict):
        return jsonify({'error': 'Некоректний результат задачі'}), 500

    pdf_path = task_result.get('path', '')
    pdf_name = task_result.get('filename', 'звіт.pdf')

    if not pdf_path or not os.path.isfile(pdf_path):
        return jsonify({'error': 'Файл не знайдено'}), 404

    # RFC 6266: filename*=UTF-8'' is sufficient; omit filename= to avoid
    # gunicorn rejecting headers with raw non-ASCII characters.
    from urllib.parse import quote
    encoded_name = quote(pdf_name, safe='')
    content_disposition = f"attachment; filename*=UTF-8''{encoded_name}"

    def stream_and_delete():
        try:
            with open(pdf_path, 'rb') as f:
                yield from iter(lambda: f.read(65536), b'')
        finally:
            try:
                os.unlink(pdf_path)
            except OSError:
                pass

    response = FlaskResponse(
        stream_and_delete(),
        mimetype='application/pdf',
        headers={'Content-Disposition': content_disposition},
    )
    return response


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
