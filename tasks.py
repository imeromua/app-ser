"""
tasks.py — Celery-задачі для фонової генерації PDF.
"""

import glob
import logging
import os
import tempfile
import time

from celery import Celery

REDIS_URL = os.environ.get('CELERY_BROKER_URL') or os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

celery = Celery(
    'app_ser',
    broker=REDIS_URL,
    backend=REDIS_URL,
)

# Celery beat: запускати cleanup кожні 30 хвилин
celery.conf.beat_schedule = {
    'cleanup-orphaned-pdfs': {
        'task': 'tasks.cleanup_orphaned_pdfs',
        'schedule': 1800,  # seconds
    },
}


@celery.task(name='tasks.cleanup_orphaned_pdfs')
def cleanup_orphaned_pdfs(max_age_seconds: int = 3600) -> int:
    """
    Видаляє тимчасові PDF-файли, старші за max_age_seconds.
    Повертає кількість видалених файлів.
    """
    tmp_dir = tempfile.gettempdir()
    now = time.time()
    removed = 0
    for path in glob.glob(os.path.join(tmp_dir, 'app_ser_*.pdf')):
        try:
            if now - os.path.getmtime(path) > max_age_seconds:
                os.unlink(path)
                removed += 1
        except OSError as exc:
            logging.debug("cleanup_orphaned_pdfs: не вдалося видалити %s: %s", path, exc)
    if removed:
        logging.info("cleanup_orphaned_pdfs: видалено %d старих PDF-файлів", removed)
    return removed


@celery.task(bind=True, time_limit=120, soft_time_limit=90)
def generate_pdf_task(self, html_string: str, pdf_name: str) -> dict:
    """
    Генерує PDF з готового HTML-рядка, зберігає у тимчасовий файл.
    Повертає словник {'path': <шлях до файлу>, 'filename': <ім'я для скачування>}.
    """
    from weasyprint import HTML  # lazy import — краш лише при виконанні задачі

    fd, path = tempfile.mkstemp(prefix='app_ser_', suffix='.pdf')
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
