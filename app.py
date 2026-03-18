#!/usr/bin/env python3
"""
Рух товарів — веб-аналізатор
Запуск: python app.py  →  http://localhost:5000
"""

from dotenv import load_dotenv
load_dotenv()

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

import datetime

from categories import CATEGORIES, detect_category
from session_store import save_session_data, load_session_data, cleanup_old_sessions
from parser import parse_xls, op_display_name
from builder import build_rows, build_summary_rows, build_document_rows
from exporter import export_excel
from importer import run_import
from reports import (
    get_inventory_template,
    get_summary_report,
    get_inventory_report,
    get_top_sales,
    get_zero_balance,
)
from tasks import celery, generate_pdf_task  # noqa: F401 — celery app must be imported

logging.basicConfig(level=logging.WARNING)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024
app.secret_key = os.environ.get('SECRET_KEY') or secrets.token_hex(32)


def _build_grand_from_rows(rows: list[dict]) -> dict:
    """Рахує grand total з рядків БД для передачі в export_excel."""
    grand = {'ПрВ': 0.0, 'Кнк': 0.0, 'ПрИ': 0.0, 'СпП': 0.0, 'Апс': 0.0,
             'Залишок': 0.0, 'Сума': 0.0,
             'Прихід': 0.0, 'Розхід': 0.0}
    for r in rows:
        for key in grand:
            val = r.get(key, 0) or 0
            try:
                grand[key] += float(val)
            except (TypeError, ValueError):
                pass
    return grand


def _bg_import(buf: bytes, filename: str) -> None:
    """Запускає run_import в фоновому потоці (щоб не гальмувати HTTP-відповідь)."""
    try:
        result = run_import(io.BytesIO(buf), filename)
        logging.info('bg_import done: %s', result)
    except Exception:
        logging.exception('bg_import failed for %s', filename)


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
        file_bufs = []  # зберігаємо буфери для фонового імпорту в БД

        for f in files:
            if not f.filename:
                continue
            raw = f.read()
            file_bufs.append((raw, secure_filename(f.filename)))
            buf = io.BytesIO(raw)
            result = parse_xls(buf)
            hdr = result['header']
            if first_header is None:
                first_header = hdr

            all_prices.update({
                a['article_id']: a['price']
                for a in result['articles']
                if a.get('price')
            })

            articles_map = {a['article_id']: a for a in result['articles']}
            records = []
            for op in result['operations']:
                art = articles_map.get(op['article_id'], {})
                d   = op['op_date']
                ym  = f"{d.year}-{d.month:02d}" if d else ''
                qty = op['qty']
                src = op['col_source']
                records.append({
                    'Артикул':    op['article_id'],
                    'Назва':      art.get('name', ''),
                    'Операція':   op_display_name(
                                      op['doc_type'],
                                      op.get('subdoc_type'),
                                      op.get('direction'),
                                  ),
                    'Документ':   op.get('doc_type', '') + '/' + op.get('doc_code', ''),
                    'Піддокумент': (
                        op.get('subdoc_type', '') + '/' + op.get('subdoc_code', '')
                        if op.get('subdoc_type') and op.get('subdoc_code')
                        else ''
                    ),
                    'Дата':       d,
                    'Рік-Місяць': ym,
                    'Кількість':  qty,
                    'Прихід':     qty if src == 'G' and qty > 0 else (qty if src == 'I' and qty > 0 else 0.0),
                    'Розхід':     abs(qty) if src == 'H' else (abs(qty) if src == 'I' and qty < 0 else 0.0),
                })
            ops_df = pd.DataFrame(records) if records else pd.DataFrame()
            all_ops.append(ops_df)

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

        # Фоновий імпорт в ПостгреСQЛ (не блокує HTTP-відповідь)
        for raw, fname in file_bufs:
            threading.Thread(target=_bg_import, args=(raw, fname), daemon=True).start()

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

    try:
        inv_rows = get_inventory_template()
    except Exception:
        logging.exception('get_inventory_template() failed, falling back to session_store')
        inv_rows = []

    if not inv_rows:
        if report_type == 'document':
            article_map: dict = {}
            for r in rows:
                if r.get('type') == 'doc_data':
                    article_map[r['Артикул']] = r
            inv_rows = list(article_map.values())
        else:
            inv_rows = [r for r in rows if r.get('type') in ('subtotal', 'summary')]

    if not inv_rows:
        return 'Немає даних для відомості інвентаризації', 400

    inv_rows.sort(key=lambda r: r.get('Назва', '').lower())

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Інвентаризація'

    NUM_COLS = 8
    col_widths = [5, 12, 56, 10, 14, 16, 30, 14]
    for ci, w in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w

    title_font   = XlFont(bold=True, color='FF0000', size=14)
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

    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=NUM_COLS)
    tc = ws.cell(row=1, column=1, value='Відомість інвентаризації')
    tc.font = title_font
    tc.alignment = XlAlign(horizontal='left', vertical='center')
    ws.row_dimensions[1].height = 22

    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=2)
    c = ws.cell(row=2, column=1, value='Маркет:')
    c.font = label_font
    c.alignment = XlAlign(vertical='center')
    c = ws.cell(row=2, column=3, value=hdr.get('shop', ''))
    c.font = value_font
    c.alignment = XlAlign(vertical='center', wrap_text=True)
    ws.merge_cells(start_row=2, start_column=3, end_row=2, end_column=NUM_COLS)

    ws.merge_cells(start_row=3, start_column=1, end_row=3, end_column=2)
    c = ws.cell(row=3, column=1, value='Єрархія:')
    c.font = label_font
    c.alignment = XlAlign(vertical='center')
    c = ws.cell(row=3, column=3, value=hdr.get('warehouse', ''))
    c.font = value_font
    c.alignment = XlAlign(vertical='center', wrap_text=True)
    ws.merge_cells(start_row=3, start_column=3, end_row=3, end_column=NUM_COLS)

    ws.merge_cells(start_row=4, start_column=1, end_row=4, end_column=2)
    c = ws.cell(row=4, column=1, value='Примітки:')
    c.font = label_font
    c.alignment = XlAlign(vertical='center')
    c = ws.cell(row=4, column=3, value=f'Позапланова інв_{category}')
    c.font = value_font
    c.alignment = XlAlign(vertical='center', wrap_text=True)
    ws.merge_cells(start_row=4, start_column=3, end_row=4, end_column=NUM_COLS)

    ws.row_dimensions[5].height = 8

    HEADER_ROW = 6
    col_headers = [
        '№', 'Артикул', 'Назва',
        'Од.\nвим.', 'База\n(залишок)',
        'Фактичні\nзалишки', 'Примітки', 'Час\nінвентаризації',
    ]
    for ci, h in enumerate(col_headers, 1):
        cell = ws.cell(row=HEADER_ROW, column=ci, value=h)
        cell.font = hdr_font
        cell.fill = hdr_fill
        cell.alignment = hdr_align
        cell.border = cell_border
    ws.row_dimensions[HEADER_ROW].height = 42

    DATA_START = HEADER_ROW + 1
    for i, r in enumerate(inv_rows, 1):
        dr = DATA_START + i - 1
        c = ws.cell(row=dr, column=1, value=i)
        c.border = cell_border
        c.alignment = center_align
        c = ws.cell(row=dr, column=2, value=r.get('Артикул', ''))
        c.border = cell_border
        c.alignment = center_align
        c.font = XlFont(bold=True)
        c = ws.cell(row=dr, column=3, value=r.get('Назва', ''))
        c.border = cell_border
        c.alignment = XlAlign(vertical='center', wrap_text=True)
        ws.cell(row=dr, column=4).border = cell_border
        zal = r.get('Залишок', '')
        c = ws.cell(row=dr, column=5)
        c.border = cell_border
        c.alignment = right_align
        c.font = XlFont(bold=True)
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
        ws.cell(row=dr, column=6).border = cell_border
        ws.cell(row=dr, column=7).border = cell_border
        ws.cell(row=dr, column=8).border = cell_border

    last_data_row = DATA_START + len(inv_rows) - 1
    fr = last_data_row + 2
    ws.merge_cells(start_row=fr, start_column=1, end_row=fr, end_column=NUM_COLS)
    ws.cell(row=fr, column=1, value='Особи, які проводили перерахунок:')
    ws.row_dimensions[fr].height = 22

    for sig_idx in range(2):
        line_row  = fr + 2 + sig_idx * 4
        label_row = line_row + 1
        for col_idx in range(3, 6):
            ws.cell(row=line_row, column=col_idx).border = bot_border
        ws.merge_cells(start_row=label_row, start_column=3, end_row=label_row, end_column=5)
        pip = ws.cell(row=label_row, column=3, value='(ПІП)')
        pip.alignment = center_align
        for col_idx in range(7, 9):
            ws.cell(row=line_row, column=col_idx).border = bot_border
        ws.merge_cells(start_row=label_row, start_column=7, end_row=label_row, end_column=8)
        sign = ws.cell(row=label_row, column=7, value='(підпис)')
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

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)

    filename = f'{safe_category}_інвентаризація.xlsx'
    return send_file(out, as_attachment=True,
                     download_name=filename,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/download_pdf')
def download_pdf():
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


# ── New navigation pages ──────────────────────────────────────────────────────

@app.route('/import')
def import_page():
    return render_template('import.html')


@app.route('/import/upload', methods=['POST'])
def import_upload():
    """DB-only import: парсить XLS та записує до PostgreSQL, показує результат інлайн."""
    files = request.files.getlist('files')
    if not files or all(not f.filename for f in files):
        return render_template('import.html', error='Файл не знайдено')

    import_results = []
    import_errors = []
    for f in files:
        if not f.filename:
            continue
        raw = f.read()
        fname = secure_filename(f.filename)
        try:
            result = run_import(io.BytesIO(raw), fname)
            import_results.append({'filename': fname, 'result': result})
        except Exception as e:
            logging.exception('import_upload failed for %s', fname)
            import_errors.append({'filename': fname, 'error': str(e)})

    return render_template('import.html',
                           import_results=import_results,
                           import_errors=import_errors)


@app.route('/reports')
def reports_page():
    return render_template('reports_form.html')


@app.route('/inventory')
def inventory_page():
    return render_template('inventory_form.html')


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.route('/api/stats')
def api_stats():
    """Повертає статистику з БД: кількість артикулів, операцій, остання дата імпорту."""
    try:
        from db import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM articles)                      AS articles,
                        (SELECT COUNT(*) FROM operations)                    AS operations,
                        (SELECT MAX(uploaded_at)::date FROM uploads)         AS last_import
                    """
                )
                row = cur.fetchone()
                last_import = row['last_import']
                return jsonify({
                    'articles': int(row['articles'] or 0),
                    'operations': int(row['operations'] or 0),
                    'last_import': last_import.isoformat() if last_import else None,
                })
    except Exception as e:
        logging.exception('api_stats error')
        return jsonify({'articles': 0, 'operations': 0, 'last_import': None, 'error': str(e)})


@app.route('/api/imports')
def api_imports():
    """Повертає останні 10 імпортів з таблиці uploads."""
    try:
        from db import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        upload_id,
                        filename,
                        shop,
                        warehouse,
                        period_from,
                        period_to,
                        uploaded_at,
                        strategy,
                        ops_inserted
                    FROM uploads
                    ORDER BY uploaded_at DESC
                    LIMIT 10
                    """
                )
                rows = []
                for row in cur.fetchall():
                    r = dict(row)
                    for k in ('period_from', 'period_to', 'uploaded_at'):
                        if r.get(k) is not None:
                            r[k] = r[k].isoformat()
                    if r.get('upload_id') is not None:
                        r['upload_id'] = str(r['upload_id'])
                    rows.append(r)
                return jsonify(rows)
    except Exception as e:
        logging.exception('api_imports error')
        return jsonify([])


@app.route('/reports/generate', methods=['POST'])
def reports_generate():
    """Генерація звіту з БД за параметрами форми."""
    report_type = request.form.get('report_type', 'summary')
    date_from_str = request.form.get('date_from', '')
    date_to_str = request.form.get('date_to', '')
    category = request.form.get('category', 'all')
    fmt = request.form.get('format', 'html')

    try:
        date_from = datetime.date.fromisoformat(date_from_str) if date_from_str else datetime.date.today().replace(day=1)
        date_to = datetime.date.fromisoformat(date_to_str) if date_to_str else datetime.date.today()
    except ValueError:
        return render_template('reports_form.html', error='Невірний формат дат')

    keywords = CATEGORIES.get(category) if category != 'all' else None

    try:
        if report_type == 'summary':
            rows = get_summary_report(date_from, date_to, keywords=keywords)
        elif report_type == 'inventory':
            rows = get_inventory_report(date_from, date_to, keywords=keywords)
        elif report_type == 'top_sales':
            rows = get_top_sales(date_from, date_to, keywords=keywords)
        elif report_type == 'zero_balance':
            rows = get_zero_balance(date_from, date_to, keywords=keywords)
        else:
            rows = get_summary_report(date_from, date_to, keywords=keywords)
    except Exception as e:
        logging.exception('reports_generate error')
        return render_template('reports_form.html', error=f'Помилка генерації звіту: {e}')

    header = {
        'shop': '',
        'warehouse': '',
        'period': f"{date_from.isoformat()} — {date_to.isoformat()}",
        'period_from': date_from,
        'period_to': date_to,
    }

    category_label = category if category != 'all' else 'Всі категорії'
    safe_cat = category_label.replace('/', '-').replace(' ', '_')
    rows_for_export = [dict(r, type='summary') for r in rows]
    grand = _build_grand_from_rows(rows_for_export)

    if fmt == 'excel':
        download_name = f"{safe_cat}_{report_type}.xlsx"
        buf = export_excel(header, rows_for_export, grand, report_type='summary')
        return send_file(buf, as_attachment=True, download_name=download_name,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    if fmt == 'pdf':
        session_id = save_session_data({
            'header': header,
            'rows': rows_for_export,
            'grand': grand,
            'filename': f"{safe_cat}_{report_type}.pdf",
            'category': category_label,
            'report_type': 'summary',
        })
        session['sid'] = session_id
        return jsonify({'redirect': '/export/pdf/start'})

    return render_template(
        'result.html',
        header=header,
        rows=rows_for_export,
        grand=grand,
        art_count=len(rows),
        row_count=len(rows),
        category=category_label,
        report_type=report_type,
    )


@app.route('/download_inventory_db')
def download_inventory_db():
    """Відомість інвентаризації з БД (без сесії)."""
    try:
        inv_rows = get_inventory_template()
    except Exception:
        logging.exception('download_inventory_db: get_inventory_template() failed')
        return 'Помилка читання даних з БД', 500

    if not inv_rows:
        return 'Немає даних в базі даних для відомості інвентаризації', 400

    inv_rows.sort(key=lambda r: r.get('Назва', '').lower())

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Інвентаризація'

    NUM_COLS = 8
    col_widths = [5, 12, 56, 10, 14, 16, 30, 14]
    for ci, w in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w

    title_font   = XlFont(bold=True, color='FF0000', size=14)
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

    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=NUM_COLS)
    tc = ws.cell(row=1, column=1, value='Відомість інвентаризації')
    tc.font = title_font
    tc.alignment = XlAlign(horizontal='left', vertical='center')
    ws.row_dimensions[1].height = 22

    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=2)
    ws.cell(row=2, column=1, value='Маркет:').font = label_font
    ws.merge_cells(start_row=2, start_column=3, end_row=2, end_column=NUM_COLS)

    ws.merge_cells(start_row=3, start_column=1, end_row=3, end_column=2)
    ws.cell(row=3, column=1, value='Єрархія:').font = label_font
    ws.merge_cells(start_row=3, start_column=3, end_row=3, end_column=NUM_COLS)

    ws.merge_cells(start_row=4, start_column=1, end_row=4, end_column=2)
    ws.cell(row=4, column=1, value='Примітки:').font = label_font
    ws.merge_cells(start_row=4, start_column=3, end_row=4, end_column=NUM_COLS)
    c = ws.cell(row=4, column=3, value='Позапланова інвентаризація')
    c.font = value_font

    ws.row_dimensions[5].height = 8

    HEADER_ROW = 6
    col_headers = [
        '№', 'Артикул', 'Назва',
        'Од.\nвим.', 'База\n(залишок)',
        'Фактичні\nзалишки', 'Примітки', 'Час\nінвентаризації',
    ]
    for ci, h in enumerate(col_headers, 1):
        cell = ws.cell(row=HEADER_ROW, column=ci, value=h)
        cell.font = hdr_font
        cell.fill = hdr_fill
        cell.alignment = hdr_align
        cell.border = cell_border
    ws.row_dimensions[HEADER_ROW].height = 42

    DATA_START = HEADER_ROW + 1
    for i, r in enumerate(inv_rows, 1):
        dr = DATA_START + i - 1
        c = ws.cell(row=dr, column=1, value=i)
        c.border = cell_border; c.alignment = center_align
        c = ws.cell(row=dr, column=2, value=r.get('Артикул', ''))
        c.border = cell_border; c.alignment = center_align; c.font = XlFont(bold=True)
        c = ws.cell(row=dr, column=3, value=r.get('Назва', ''))
        c.border = cell_border; c.alignment = XlAlign(vertical='center', wrap_text=True)
        ws.cell(row=dr, column=4).border = cell_border
        zal = r.get('Залишок', '')
        c = ws.cell(row=dr, column=5)
        c.border = cell_border; c.alignment = right_align; c.font = XlFont(bold=True)
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
        ws.cell(row=dr, column=6).border = cell_border
        ws.cell(row=dr, column=7).border = cell_border
        ws.cell(row=dr, column=8).border = cell_border

    last_data_row = DATA_START + len(inv_rows) - 1
    fr = last_data_row + 2
    ws.merge_cells(start_row=fr, start_column=1, end_row=fr, end_column=NUM_COLS)
    ws.cell(row=fr, column=1, value='Особи, які проводили перерахунок:')
    ws.row_dimensions[fr].height = 22

    for sig_idx in range(2):
        line_row  = fr + 2 + sig_idx * 4
        label_row = line_row + 1
        for col_idx in range(3, 6):
            ws.cell(row=line_row, column=col_idx).border = bot_border
        ws.merge_cells(start_row=label_row, start_column=3, end_row=label_row, end_column=5)
        ws.cell(row=label_row, column=3, value='(ПІП)').alignment = center_align
        for col_idx in range(7, 9):
            ws.cell(row=line_row, column=col_idx).border = bot_border
        ws.merge_cells(start_row=label_row, start_column=7, end_row=label_row, end_column=8)
        ws.cell(row=label_row, column=7, value='(підпис)').alignment = center_align

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

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    today = datetime.date.today().isoformat()
    return send_file(out, as_attachment=True,
                     download_name=f'інвентаризація_{today}.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
