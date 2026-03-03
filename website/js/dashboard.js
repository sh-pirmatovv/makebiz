(function () {
  const state = {
    limit: 25,
    offset: 0,
    total: 0,
  };

  const FIELD_IDS = [
    'source_url', 'company_name', 'short_name', 'inn', 'registration_date', 'activity_status',
    'registration_authority', 'thsht', 'dbibt', 'ifut', 'charter_capital_uzs', 'email', 'phone',
    'address', 'region', 'district', 'category', 'tax_committee', 'large_taxpayer', 'director',
    'founders', 'employees_count', 'branch_count'
  ];

  async function jget(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`${url} failed`);
    return r.json();
  }

  async function jsend(url, method, payload) {
    const r = await fetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!r.ok) {
      const msg = await r.text();
      throw new Error(msg || `${url} failed`);
    }
    return r.json();
  }

  function metricCard(label, value) {
    return `<article class="metric"><p class="label">${label}</p><p class="value">${value}</p></article>`;
  }

  function barRow(label, value, max) {
    const width = max > 0 ? Math.round((value / max) * 100) : 0;
    return `<div><small>${label} (${value})</small><div class="bar"><span style="width:${width}%"></span></div></div>`;
  }

  function scoreBadge(score) {
    if (score >= 80) return 'badge badge-high';
    if (score >= 65) return 'badge badge-mid';
    return 'badge badge-low';
  }

  function renderSimpleTable(tableId, rows, cellsFn, emptyText) {
    const body = document.querySelector(`${tableId} tbody`);
    if (!body) return;
    if (!rows.length) {
      body.innerHTML = `<tr><td colspan="8">${emptyText}</td></tr>`;
      return;
    }
    body.innerHTML = rows.map((row) => `<tr>${cellsFn(row)}</tr>`).join('');
  }

  function renderSummary(summary) {
    const root = document.getElementById('summaryCards');
    if (!root) return;
    root.innerHTML = [
      metricCard('Total Companies', summary.total_companies),
      metricCard('Open RFQ', summary.open_rfq),
      metricCard('Regions', summary.regions),
      metricCard('Legal Forms', summary.legal_forms || 0),
      metricCard('Avg Score', summary.avg_score),
      metricCard('Dedup Conflicts', summary.dedup_conflicts),
      metricCard('Relations', summary.relations),
      metricCard('Quality Contacts %', summary.with_contacts_pct || 0),
      metricCard('Quality Category %', summary.with_category_pct || 0),
    ].join('');
  }

  function renderScoring(scoring) {
    const root = document.getElementById('scoringBars');
    if (!root) return;
    const max = Math.max(scoring.high || 0, scoring.medium || 0, scoring.low || 0, 1);
    root.innerHTML = [
      barRow('High (80+)', scoring.high || 0, max),
      barRow('Medium (65-79)', scoring.medium || 0, max),
      barRow('Low (<65)', scoring.low || 0, max),
    ].join('');
  }

  function renderQuality(quality) {
    const root = document.getElementById('qualityBars');
    if (!root) return;
    root.innerHTML = [
      barRow('Contacts %', quality.with_contacts_pct || 0, 100),
      barRow('Region %', quality.with_region_pct || 0, 100),
      barRow('Category %', quality.with_category_pct || 0, 100),
      barRow('Conflicts', quality.conflicts || 0, Math.max(1, quality.conflicts || 0)),
    ].join('');
  }

  function renderDeepTables(data) {
    renderSimpleTable('#categoriesTable', data.categories || [], (x) => `
      <td>${x.category}</td><td>${x.companies}</td><td>${x.avg_score ?? '-'}</td>
    `, 'No categories');

    renderSimpleTable('#statusTable', data.statuses || [], (x) => `
      <td>${x.activity_status}</td><td>${x.companies}</td>
    `, 'No statuses');

    renderSimpleTable('#missingTable', data.missing_fields || [], (x) => `
      <td>${x.field}</td><td>${x.missing}</td>
    `, 'No missing metrics');

    renderSimpleTable('#topCompaniesTable', data.top_companies || [], (x) => `
      <td>${x.company_name}</td><td>${x.inn}</td><td>${x.region}</td><td><span class="${scoreBadge(x.score || 0)}">${x.score || 0}</span></td><td>${x.updated_at || '-'}</td>
    `, 'No records');

    renderSimpleTable('#weakCompaniesTable', data.weakest_companies || [], (x) => `
      <td>${x.company_name}</td><td>${x.inn}</td><td>${x.region}</td><td><span class="${scoreBadge(x.score || 0)}">${x.score || 0}</span></td><td>${x.updated_at || '-'}</td>
    `, 'No records');

    renderSimpleTable('#recentUpdatesTable', data.recent_updates || [], (x) => `
      <td>${x.id}</td><td>${x.company_name}</td><td>${x.inn}</td><td>${x.category}</td><td>${x.region}</td>
      <td><span class="${scoreBadge(x.score || 0)}">${x.score || 0}</span></td><td>${x.updated_at || '-'}</td>
      <td><button class="btn btn-soft editor-open" data-company-id="${x.id}">Edit</button></td>
    `, 'No updates');

    renderSimpleTable('#conflictsTable', data.open_conflicts || [], (x) => `
      <td>${x.id}</td><td>${x.reason}</td><td>${x.confidence}</td><td>${x.status}</td><td>${x.company_a}</td><td>${x.company_b}</td>
    `, 'No conflicts');
  }

  async function loadRegions() {
    const data = await jget('/api/dashboard/regions?limit=30');
    renderSimpleTable('#regionsTable', data.items || [], (x) => `
      <td>${x.region}</td><td>${x.companies}</td><td>${x.avg_score}</td>
    `, 'No regions');
  }

  async function loadRuns() {
    const data = await jget('/api/dashboard/pipeline-runs?limit=30');
    renderSimpleTable('#runsTable', data.items || [], (x) => `
      <td>${x.id}</td><td>${x.source_name}</td><td>${x.status}</td><td>${x.rows_in}</td><td>${x.rows_out}</td><td>${x.started_at}</td><td>${x.finished_at || '-'}</td>
    `, 'No runs');
  }

  async function loadDeepDashboard() {
    const deep = await jget('/api/dashboard/deep');
    const summary = { ...(deep.summary || {}), ...(deep.quality || {}) };
    renderSummary(summary);
    renderScoring(deep.scoring || {});
    renderQuality(deep.quality || {});
    renderDeepTables(deep);
  }

  async function loadEditorTable() {
    const q = (document.getElementById('editorQuery')?.value || '').trim();
    const region = (document.getElementById('editorRegion')?.value || '').trim();
    const sector = (document.getElementById('editorSector')?.value || '').trim();
    const orderBy = document.getElementById('editorSort')?.value || 'score_desc';

    const params = new URLSearchParams({
      limit: String(state.limit),
      offset: String(state.offset),
      order_by: orderBy,
    });
    if (q) params.set('q', q);
    if (region) params.set('region', region);
    if (sector) params.set('sector', sector);

    const data = await jget(`/api/companies?${params.toString()}`);
    state.total = data.total || 0;

    renderSimpleTable('#editorTable', data.items || [], (x) => `
      <td>${x.id}</td><td>${x.company_name}</td><td>${x.inn}</td><td>${x.region}</td><td>${x.category}</td>
      <td><span class="${scoreBadge(x.score || 0)}">${x.score || 0}</span></td><td>${x.updated_at || '-'}</td>
      <td><button class="btn btn-soft editor-open" data-company-id="${x.id}">Edit</button></td>
    `, 'No companies');

    const page = Math.floor(state.offset / state.limit) + 1;
    const totalPages = Math.max(1, Math.ceil(state.total / state.limit));
    const info = document.getElementById('editorPageInfo');
    if (info) info.textContent = `Page ${page} / ${totalPages} (Total ${state.total})`;

    const prev = document.getElementById('editorPrev');
    const next = document.getElementById('editorNext');
    if (prev) prev.disabled = state.offset === 0;
    if (next) next.disabled = state.offset + state.limit >= state.total;
  }

  async function openEditor(companyId) {
    const data = await jget(`/api/companies/${companyId}`);
    const company = data.company || {};
    FIELD_IDS.forEach((field) => {
      const input = document.getElementById(`edit_${field}`);
      if (!input) return;
      const val = company[field];
      input.value = val == null || val === '-' ? '' : String(val);
    });
    const idInput = document.getElementById('edit_id');
    if (idInput) idInput.value = String(companyId);

    const modal = document.getElementById('editorModalBackdrop');
    if (modal) modal.classList.add('active');
  }

  function closeEditor() {
    const modal = document.getElementById('editorModalBackdrop');
    if (modal) modal.classList.remove('active');
  }

  async function submitEditor(e) {
    e.preventDefault();
    const id = Number(document.getElementById('edit_id')?.value || 0);
    if (!id) return;

    const payload = {};
    FIELD_IDS.forEach((field) => {
      const input = document.getElementById(`edit_${field}`);
      if (!input) return;
      payload[field] = input.value;
    });

    await jsend(`/api/companies/${id}`, 'PUT', payload);
    closeEditor();
    await Promise.all([loadEditorTable(), loadDeepDashboard()]);
  }

  function setupEditor() {
    const apply = document.getElementById('editorApply');
    if (apply) {
      apply.addEventListener('click', async () => {
        state.offset = 0;
        await loadEditorTable();
      });
    }

    const prev = document.getElementById('editorPrev');
    const next = document.getElementById('editorNext');
    if (prev) {
      prev.addEventListener('click', async () => {
        state.offset = Math.max(0, state.offset - state.limit);
        await loadEditorTable();
      });
    }
    if (next) {
      next.addEventListener('click', async () => {
        if (state.offset + state.limit < state.total) {
          state.offset += state.limit;
          await loadEditorTable();
        }
      });
    }

    document.addEventListener('click', (e) => {
      const btn = e.target.closest('.editor-open');
      if (!btn) return;
      const companyId = Number(btn.getAttribute('data-company-id') || 0);
      if (companyId) openEditor(companyId).catch(() => null);
    });

    const modal = document.getElementById('editorModalBackdrop');
    if (modal) {
      modal.addEventListener('click', (e) => {
        if (e.target === modal) closeEditor();
      });
    }

    const cancel = document.getElementById('editorCancel');
    if (cancel) cancel.addEventListener('click', closeEditor);

    const form = document.getElementById('editorForm');
    if (form) form.addEventListener('submit', (e) => submitEditor(e).catch(() => null));
  }

  async function checkApi() {
    const h = document.getElementById('healthStatus');
    const c = document.getElementById('companiesApiStatus');
    const r = document.getElementById('rfqApiStatus');

    try {
      await jget('/api/health');
      if (h) h.textContent = 'OK';
    } catch (_e) {
      if (h) h.textContent = 'ERROR';
    }

    try {
      const data = await jget('/api/companies?limit=1');
      if (c) c.textContent = `OK (${data.total || data.count || 0} total)`;
    } catch (_e) {
      if (c) c.textContent = 'ERROR';
    }

    try {
      const data = await jget('/api/rfq?limit=1');
      if (r) r.textContent = `OK (${(data.items || []).length} sample)`;
    } catch (_e) {
      if (r) r.textContent = 'ERROR';
    }
  }

  function renderLogs(items) {
    const box = document.getElementById('opsLogWindow');
    if (!box) return;
    if (!items || !items.length) {
      box.innerHTML = '<div class="log-line">No logs yet</div>';
      return;
    }
    box.innerHTML = items
      .map((x) => {
        const ts = x.ts || '-';
        const level = x.level || 'INFO';
        const action = x.action || 'event';
        const msg = x.message || '';
        const extra = x.extra ? `\n${JSON.stringify(x.extra)}` : '';
        return `<div class="log-line">[${ts}] [${level}] ${action} | ${msg}${extra}</div>`;
      })
      .join('');
    box.scrollTop = box.scrollHeight;
  }

  async function loadLogs() {
    try {
      const data = await jget('/api/logs?limit=400');
      renderLogs(data.items || []);
    } catch (_e) {
      renderLogs([{ ts: '-', level: 'ERROR', action: 'logs', message: 'Failed to load logs' }]);
    }
  }

  async function boot() {
    if (!location.pathname.endsWith('/dashboard.html') && !location.pathname.endsWith('/')) return;
    setupEditor();
    await Promise.all([loadDeepDashboard(), loadRegions(), loadRuns(), loadEditorTable(), checkApi(), loadLogs()]);
    setInterval(() => {
      loadLogs().catch(() => null);
      checkApi().catch(() => null);
    }, 10000);
  }

  document.addEventListener('DOMContentLoaded', () => {
    boot().catch(() => null);
  });
})();
