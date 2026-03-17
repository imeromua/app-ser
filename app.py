#!/usr/bin/env python3
"""
Рух товарів — веб-аналізатор
Запуск: python app.py  →  http://localhost:5000
"""

import io
import logging
import os
import secrets

from flask import Flask, request, render_template, send_file, session, Response
from werkzeug.utils import secure_filename
import pandas as pd

from categories import detect_category
from session_store import save_session_data, load_session_data, cleanup_old_sessions
from parser import parse_xls
from builder import build_rows, build_summary_rows, build_document_rows
from exporter import export_excel

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

    return Response(
        pdf_bytes,
        mimetype='application/pdf',
        headers={'Content-Disposition': f'attachment; filename="{pdf_name}"'},
    )


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
