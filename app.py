#!/usr/bin/env python3
"""
Рух товарів — веб-аналізатор
Запуск: python app.py  →  http://localhost:5000
"""

import io
import logging
import os
import secrets
import uuid

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
        cleanup_old_sessions()

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


@app.route('/download_pdf')
def download_pdf():
    # DEPRECATED: цей маршрут блокує Flask-воркер під час генерації PDF.
    # Використовуйте /export/pdf/start → /export/pdf/status → /export/pdf/result.
    try:
        from weasyprint import HTML
    except ImportError:
        return 'WeasyPrint не встановлено. Виконайте: pip install weasyprint', 500

    sid = session.get('sid')
    data = load_session_data(sid)
    if not data:
        return 'Немає даних або сесія застаріла', 400

    header      = data['header']
    rows        = data['rows']
    grand       = data['grand']
    report_type = data.get('report_type', 'detail')
    category    = data.get('category', '')

    html_content = render_template(
        'report_pdf.html',
        header=header,
        rows=rows,
        grand=grand,
        report_type=report_type,
        category=category,
    )
    pdf_bytes = HTML(string=html_content).write_pdf()
    pdf_name  = data.get('filename', 'звіт').replace('.xlsx', '.pdf')

    pdf_io = io.BytesIO(pdf_bytes)
    pdf_io.seek(0)

    return send_file(
        pdf_io,
        as_attachment=True,
        download_name=pdf_name,
        mimetype='application/pdf'
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
