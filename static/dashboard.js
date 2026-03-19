// Earliest date to use for "all time" range queries
const DATE_RANGE_ALL_START = '2000-01-01';

// ── Tab switching ──────────────────────────────────────────────────────────

function switchTab(tabName) {
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.dash-tab-btn, .bottom-nav-item').forEach(b => b.classList.remove('active'));

  const pane = document.getElementById('tab-' + tabName);
  if (pane) pane.classList.add('active');

  document.querySelectorAll('[data-tab="' + tabName + '"]').forEach(b => b.classList.add('active'));

  if (tabName === 'dbstatus') loadDbStatus();
  if (tabName === 'inventory') loadCategories();
  if (tabName === 'import') loadImportHistory();

  history.replaceState(null, '', '#' + tabName);
}

document.addEventListener('DOMContentLoaded', () => {
  // Init tab from hash or default
  const hash = location.hash.replace('#', '') || 'import';
  switchTab(hash);

  // Tab buttons
  document.querySelectorAll('.dash-tab-btn, .bottom-nav-item').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });

  initImportTab();
  initReportsTab();
});

// ── Tab 1: Import ──────────────────────────────────────────────────────────

function initImportTab() {
  const dropZone   = document.getElementById('drop-zone');
  const fileInput  = document.getElementById('file-input');
  const filePreview = document.getElementById('file-preview');
  const importBtn  = document.getElementById('import-btn');
  const spinner    = document.getElementById('import-spinner');
  const resultCard = document.getElementById('import-result');

  if (!dropZone) return;

  dropZone.addEventListener('click', () => fileInput.click());

  dropZone.addEventListener('dragover', e => {
    e.preventDefault();
    dropZone.classList.add('drag-over');
  });

  dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));

  dropZone.addEventListener('drop', e => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    const files = e.dataTransfer.files;
    if (files.length) setFile(files[0]);
  });

  fileInput.addEventListener('change', () => {
    if (fileInput.files.length) setFile(fileInput.files[0]);
  });

  importBtn.addEventListener('click', doImport);

  function setFile(file) {
    filePreview.querySelector('.file-preview-name').textContent = file.name;
    filePreview.querySelector('.file-preview-size').textContent = formatBytes(file.size);
    filePreview.classList.add('show');
    importBtn.disabled = false;
  }

  async function doImport() {
    const file = fileInput.files[0];
    if (!file) return;

    importBtn.disabled = true;
    spinner.classList.add('show');
    resultCard.className = 'result-card';

    const fd = new FormData();
    fd.append('file', file);

    try {
      const resp = await fetch('/import', { method: 'POST', body: fd });
      const data = await resp.json();

      spinner.classList.remove('show');
      importBtn.disabled = false;

      if (data.error) {
        showResult('error', '❌ Помилка імпорту', [['Деталі', data.error]]);
        return;
      }

      if (data.strategy === 'SKIP') {
        showResult('warning', '⏭️ Імпорт пропущено', [
          ['Причина', 'Дані вже є в базі'],
          ['Кінець звіту', fmtDate(data.period_to)],
          ['Макс. дата в БД', fmtDate(data.db_max_date)],
        ]);
        return;
      }

      const rows = [
        ['📅 Стратегія', data.strategy],
        ['➕ Додано операцій', fmtNum(data.ops_inserted)],
        ['📦 Артикулів', fmtNum(data.articles_count)],
      ];
      if (data.period_from) rows.push(['📅 Від', fmtDate(data.period_from)]);
      if (data.period_to)   rows.push(['📅 До',  fmtDate(data.period_to)]);
      rows.push(['⚠️ Розбіжностей', data.invalid_count]);

      showResult('success', '✅ Імпорт завершено', rows);

      if (data.invalid_count > 0) {
        const warn = document.getElementById('import-warn');
        if (warn) {
          warn.querySelector('.warn-list').textContent =
            data.invalid_snapshots.map(s => s.article_id).join(', ');
          warn.classList.add('show');
        }
      }

      loadImportHistory();
    } catch (err) {
      spinner.classList.remove('show');
      importBtn.disabled = false;
      showResult('error', '❌ Помилка запиту', [['Деталі', err.message]]);
    }
  }

  function showResult(type, title, rows) {
    resultCard.className = 'result-card show ' + type;
    resultCard.querySelector('.result-title').textContent = title;
    const body = resultCard.querySelector('.result-body');
    body.innerHTML = rows.map(([label, val]) =>
      `<div class="result-row"><span class="label">${label}:</span><span class="value">${val ?? '—'}</span></div>`
    ).join('');
  }
}

async function loadImportHistory() {
  const tbody = document.querySelector('#imports-table tbody');
  if (!tbody) return;

  tbody.innerHTML = '<tr><td colspan="7" class="text-center py-3"><span class="text-muted">Завантаження...</span></td></tr>';

  try {
    const resp = await fetch('/api/uploads');
    const data = await resp.json();

    if (!Array.isArray(data) || data.length === 0) {
      tbody.innerHTML = '<tr><td colspan="7" class="text-center py-3"><span class="text-muted">Немає імпортів</span></td></tr>';
      return;
    }

    tbody.innerHTML = data.map((r, i) => `
      <tr>
        <td>${i + 1}</td>
        <td title="${r.filename || ''}">${truncate(r.filename || '—', 22)}</td>
        <td>${r.shop || '—'}</td>
        <td>${fmtDate(r.period_from)} – ${fmtDate(r.period_to)}</td>
        <td>${fmtNum(r.ops_inserted)}</td>
        <td>${fmtDatetime(r.uploaded_at)}</td>
        <td><span class="badge ${r.strategy === 'FULL_INSERT' ? 'bg-success' : 'bg-info'} text-dark">${r.strategy || '—'}</span></td>
      </tr>
    `).join('');
  } catch {
    tbody.innerHTML = '<tr><td colspan="7" class="text-center py-3 text-danger">Помилка завантаження</td></tr>';
  }
}

// ── DB clear modal ──────────────────────────────────────────────────────────

function openClearModal() {
  document.getElementById('clear-modal').classList.add('show');
}

function closeClearModal() {
  document.getElementById('clear-modal').classList.remove('show');
}

async function confirmClear() {
  closeClearModal();
  try {
    const resp = await fetch('/db/clear', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ confirm_token: 'CONFIRM_CLEAR' }),
    });
    const data = await resp.json();
    alert(data.success ? '✅ ' + data.message : '❌ ' + data.error);
    if (data.success) loadImportHistory();
  } catch (e) {
    alert('❌ Помилка: ' + e.message);
  }
}

// ── Tab 2: Reports ─────────────────────────────────────────────────────────

function initReportsTab() {
  // Quick date buttons
  document.querySelectorAll('.date-quick button').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.date-quick button').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const [from, to] = getDateRange(btn.dataset.range);
      const df = document.getElementById('report-date-from');
      const dt = document.getElementById('report-date-to');
      if (df) df.value = from;
      if (dt) dt.value = to;
    });
  });

  // Report type cards
  document.querySelectorAll('.report-type-card').forEach(card => {
    card.addEventListener('click', () => {
      document.querySelectorAll('.report-type-card').forEach(c => c.classList.remove('active'));
      card.classList.add('active');
    });
  });

  loadCategoriesForReports();
}

function getDateRange(range) {
  const now = new Date();
  const pad  = n => String(n).padStart(2, '0');
  const fmt  = d => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;

  if (range === 'today') {
    const s = fmt(now);
    return [s, s];
  }
  if (range === 'week') {
    const mon = new Date(now);
    mon.setDate(now.getDate() - now.getDay() + (now.getDay() === 0 ? -6 : 1));
    return [fmt(mon), fmt(now)];
  }
  if (range === 'month') {
    return [`${now.getFullYear()}-${pad(now.getMonth()+1)}-01`, fmt(now)];
  }
  if (range === 'year') {
    return [`${now.getFullYear()}-01-01`, fmt(now)];
  }
  // all
  return [DATE_RANGE_ALL_START, fmt(now)];
}

async function loadCategoriesForReports() {
  const sel = document.getElementById('report-category');
  if (!sel) return;
  try {
    const resp = await fetch('/api/categories');
    const data = await resp.json();
    if (Array.isArray(data)) {
      data.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c.category;
        opt.textContent = `${c.category} (${c.count})`;
        sel.appendChild(opt);
      });
    }
  } catch { /* ignore */ }
}

async function exportReport() {
  const dateFrom = document.getElementById('report-date-from')?.value;
  const dateTo   = document.getElementById('report-date-to')?.value;

  if (!dateFrom || !dateTo) {
    alert('Оберіть діапазон дат');
    return;
  }

  const form = document.getElementById('export-form');
  if (form) {
    form.querySelector('[name="date_from"]').value = dateFrom;
    form.querySelector('[name="date_to"]').value   = dateTo;
    form.submit();
  }
}

// ── Tab 3: Inventory ───────────────────────────────────────────────────────

async function loadCategories() {
  const grid = document.getElementById('category-grid');
  if (!grid) return;

  grid.innerHTML = skeletonCards(6);

  try {
    const resp = await fetch('/api/categories');
    const data = await resp.json();

    if (!Array.isArray(data) || data.length === 0) {
      grid.innerHTML = `<div class="empty-state"><i class="bi bi-database-x"></i>
        <p>⚠️ База даних порожня. Спочатку виконайте імпорт.</p></div>`;
      return;
    }

    const icons = {
      'Рибна продукція': 'bi-fish',
      'Молочна продукція': 'bi-cup-straw',
      'Сирна продукція': 'bi-egg-fried',
      'Ковбасна продукція': 'bi-bag',
      'Овочі та фрукти': 'bi-apple',
      'Алкогольні напої': 'bi-cup',
      'Безалкогольні напої': 'bi-droplet',
      'Бакалея': 'bi-basket',
      'Кондитерські вироби': 'bi-cake',
      'Цукерки та солодощі': 'bi-heart',
      'Консерви': 'bi-archive',
      'Напівфабрикати': 'bi-box',
      'Змішана продукція': 'bi-grid',
    };

    grid.innerHTML = data.map(c => `
      <div class="category-card">
        <i class="bi ${icons[c.category] || 'bi-tag'}" style="font-size:1.6rem;color:var(--accent-amber)"></i>
        <div class="cat-name">${c.category}</div>
        <div class="cat-count">${fmtNum(c.count)} артикулів</div>
        <a href="/download_inventory_db?category=${encodeURIComponent(c.category)}"
           class="btn btn-amber" style="margin-top:10px;font-size:.8rem;padding:6px 12px;text-decoration:none;border-radius:8px;display:inline-flex;align-items:center;gap:5px">
          <i class="bi bi-file-earmark-arrow-down"></i> Сформувати відомість
        </a>
      </div>
    `).join('');
  } catch (e) {
    grid.innerHTML = `<div class="empty-state text-danger"><i class="bi bi-exclamation-triangle"></i><p>${e.message}</p></div>`;
  }
}

function skeletonCards(n) {
  return Array.from({ length: n }, () => `
    <div class="category-card">
      <div class="skeleton" style="height:1.6rem;width:1.6rem;border-radius:50%"></div>
      <div class="skeleton skeleton-text" style="width:70%;margin-top:8px"></div>
      <div class="skeleton skeleton-text" style="width:50%"></div>
      <div class="skeleton" style="height:32px;margin-top:8px;border-radius:8px"></div>
    </div>
  `).join('');
}

// ── Tab 4: DB Status ───────────────────────────────────────────────────────

async function loadDbStatus() {
  setKpiSkeleton();

  try {
    const resp = await fetch('/api/db_status');
    const d = await resp.json();

    if (d.error) {
      document.getElementById('kpi-articles').textContent = '—';
      document.getElementById('kpi-ops').textContent = '—';
      document.getElementById('kpi-days').textContent = '—';
      document.getElementById('kpi-sum').textContent = '—';
      return;
    }

    document.getElementById('kpi-articles').textContent = fmtNum(d.articles_count);
    document.getElementById('kpi-ops').textContent      = fmtNum(d.ops_count);
    document.getElementById('kpi-days').textContent     = fmtNum(d.days_in_db);
    document.getElementById('kpi-sum').textContent      = fmtMoney(d.total_sum);

    // Op type badges
    const opBadges = document.getElementById('op-badges');
    if (opBadges && d.by_type) {
      const typeLabels = {
        'ПрВ': 'Прихід (ПрВ)',
        'Кнк': 'Продажі (Кнк)',
        'Воз': 'Повернення (Воз)',
        'СпП': 'Списання (СпП)',
        'ПрИ': 'Переміщення (ПрИ)',
        'Апс': 'Коригування (Апс)',
      };
      opBadges.innerHTML = Object.entries(d.by_type).map(([type, qty]) =>
        `<div class="op-badge"><span class="op-type">${typeLabels[type] || type}:</span><span class="op-val">${fmtNum(qty)}</span></div>`
      ).join('');
    }

    // Top articles table
    const tbody = document.querySelector('#top-articles-table tbody');
    if (tbody && d.top_articles) {
      tbody.innerHTML = d.top_articles.map((r, i) => `
        <tr>
          <td>${i+1}</td>
          <td>${r.article_id}</td>
          <td>${r.name}</td>
          <td>${fmtNum(r.balance)}</td>
          <td>${fmtMoney(r.price)}</td>
          <td>${fmtMoney(r.balance_sum)}</td>
        </tr>
      `).join('');
    }

    // Footer info
    setEl('db-date-min',     fmtDate(d.date_min));
    setEl('db-date-max',     fmtDate(d.date_max));
    setEl('db-last-import',  fmtDatetime(d.last_import));
  } catch (e) {
    document.getElementById('kpi-articles').textContent = 'ERR';
    console.error(e);
  }
}

function setKpiSkeleton() {
  ['kpi-articles', 'kpi-ops', 'kpi-days', 'kpi-sum'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.innerHTML = '<div class="skeleton skeleton-kpi"></div>';
  });
}

// ── Helpers ────────────────────────────────────────────────────────────────

function fmtNum(n) {
  if (n == null || n === '') return '—';
  return Number(n).toLocaleString('uk-UA');
}

function fmtMoney(n) {
  if (n == null || n === '') return '—';
  return Number(n).toLocaleString('uk-UA', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtDate(s) {
  if (!s) return '—';
  const d = new Date(s);
  if (isNaN(d)) return s;
  const p = n => String(n).padStart(2, '0');
  return `${p(d.getDate())}.${p(d.getMonth()+1)}.${d.getFullYear()}`;
}

function fmtDatetime(s) {
  if (!s) return '—';
  const d = new Date(s);
  if (isNaN(d)) return s;
  const p = n => String(n).padStart(2, '0');
  return `${p(d.getDate())}.${p(d.getMonth()+1)}.${d.getFullYear()} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

function formatBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1024*1024) return (b/1024).toFixed(1) + ' KB';
  return (b/(1024*1024)).toFixed(1) + ' MB';
}

function truncate(s, n) {
  return s.length > n ? s.slice(0, n) + '…' : s;
}

function setEl(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}
