"""
tasks.py — Celery-задачі для фонової генерації PDF.
"""

import os
import tempfile

from celery import Celery
from weasyprint import HTML

REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

celery = Celery(
    'app_ser',
    broker=REDIS_URL,
    backend=REDIS_URL,
)


@celery.task(bind=True, time_limit=120, soft_time_limit=90)
def generate_pdf_task(self, html_string: str, pdf_name: str) -> dict:
    """
    Генерує PDF з готового HTML-рядка, зберігає у тимчасовий файл.
    Повертає словник {'path': <шлях до файлу>, 'filename': <ім'я для скачування>}.
    """
    fd, path = tempfile.mkstemp(suffix='.pdf')
    os.close(fd)
    try:
        HTML(string=html_string).write_pdf(path)
    except Exception:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    return {'path': path, 'filename': pdf_name}
