(function () {
  const I18N = window.MAKEBIZ_I18N || {};
  const CONTAINER_VW_KEY = 'makebiz_container_vw';

  const companies = [
    { id: 1, name: 'Sam Agro Trade', inn: '304000112', region: 'Samarqand', okedCode: '01161', okedName: 'Cotton growing', thshtCode: '130', thshtText: 'Fermer xo`jaligi', legalForm: 'fermer xo`jaligi', status: 'Active', stability: "O'rta", score: 82 },
    { id: 2, name: 'Tash Build Group', inn: '309118220', region: 'Toshkent', okedCode: '41201', okedName: 'Residential building construction', thshtCode: '152', thshtText: 'Mas`uliyati cheklangan jamiyat', legalForm: 'mas`uliyati cheklangan jamiyati', status: 'Active', stability: 'Yuqori', score: 91 },
    { id: 3, name: 'Silk Road Logistics', inn: '302200541', region: 'Andijon', okedCode: '46900', okedName: 'Non-specialized wholesale', thshtCode: '152', thshtText: 'Mas`uliyati cheklangan jamiyat', legalForm: 'mas`uliyati cheklangan jamiyati', status: 'Active', stability: 'Qoniqarli', score: 76 },
    { id: 4, name: 'Delta Retail Supply', inn: '306114090', region: "Farg'ona", okedCode: '47190', okedName: 'Retail in non-specialized stores', thshtCode: '110', thshtText: 'Xususiy korxona', legalForm: 'xususiy korxonasi', status: 'Moderate', stability: 'Quyi', score: 68 },
    { id: 5, name: 'Bukhara Tech Service', inn: '301554773', region: 'Buxoro', okedCode: '62020', okedName: 'IT consulting', thshtCode: '152', thshtText: 'Mas`uliyati cheklangan jamiyat', legalForm: 'mas`uliyati cheklangan jamiyati', status: 'Moderate', stability: "O'rta", score: 72 },
    { id: 6, name: 'Nukus Energy Parts', inn: '308341559', region: "Qoraqalpog'iston", okedCode: '46690', okedName: 'Wholesale other machinery', thshtCode: '110', thshtText: 'Xususiy korxona', legalForm: 'xususiy korxonasi', status: 'Low Activity', stability: 'Quyi', score: 53 },
  ];

  const rfqSeed = [
    { title: 'Packaging for FMCG line', stage: 'Sourcing', budget: '220,000,000 UZS' },
    { title: 'Cold-chain transport contract', stage: 'Negotiation', budget: '160,000,000 UZS' },
    { title: 'ERP integration partner', stage: 'Offer Review', budget: '145,000,000 UZS' },
  ];

  const categoriesTree = [
    { name: 'Agro', subs: ['Irrigation systems', 'Seeds & Fertilizers', 'Storage', 'Processing', 'Export services'] },
    { name: 'Construction', subs: ['Cement & Concrete', 'Metal structures', 'Engineering', 'Road works', 'Architect services'] },
    { name: 'Logistics', subs: ['FTL/LTL transport', 'Cold chain', 'Customs brokers', 'Warehouse 3PL', 'Cross-border ops'] },
    { name: 'IT Services', subs: ['ERP integrators', 'CRM automation', 'Data integration', 'Cybersecurity', 'Cloud migration'] },
    { name: 'Manufacturing', subs: ['Textile', 'Food production', 'Electronics assembly', 'Packaging', 'Industrial equipment'] },
    { name: 'B2B Retail', subs: ['Wholesale', 'Distributor networks', 'Sourcing partners', 'Procurement ops', 'Channel analytics'] },
    { name: 'Energy', subs: ['Solar components', 'Power equipment', 'Industrial electrics', 'Maintenance', 'Energy audit'] },
    { name: 'Import/Export', subs: ['Trade compliance', 'International sourcing', 'Export sales', 'Trade finance', 'Incoterms support'] },
  ];

  function getLang() {
    const cached = localStorage.getItem('makebiz_lang');
    if (cached && I18N[cached]) return cached;
    return 'ru';
  }

  function t(lang, key) {
    const dict = I18N[lang] || I18N.ru || {};
    return dict[key] || key;
  }

  function setLang(lang) {
    if (!I18N[lang]) return;
    localStorage.setItem('makebiz_lang', lang);
    applyI18n(lang);
  }

  function clampContainerVw(value) {
    const n = Number(value);
    if (Number.isNaN(n)) return 93;
    return Math.max(82, Math.min(98, Math.round(n)));
  }

  function applyContainerWidth(vw) {
    const safe = clampContainerVw(vw);
    document.documentElement.style.setProperty('--container-vw', String(safe));
    localStorage.setItem(CONTAINER_VW_KEY, String(safe));
    const slider = document.getElementById('containerWidthSlider');
    const value = document.getElementById('containerWidthValue');
    if (slider) slider.value = String(safe);
    if (value) value.textContent = `${safe}%`;
  }

  function setupContainerWidthControl() {
    const stored = localStorage.getItem(CONTAINER_VW_KEY);
    applyContainerWidth(stored || 93);
    const slider = document.getElementById('containerWidthSlider');
    if (!slider) return;
    slider.addEventListener('input', (e) => applyContainerWidth(e.target.value));
  }

  function scoreBadge(score) {
    if (score >= 80) return 'badge badge-high';
    if (score >= 65) return 'badge badge-mid';
    return 'badge badge-low';
  }

  function stabilityBadge(value) {
    const v = (value || '').toLowerCase();
    if (v.includes('yuqori') || v.includes('high')) return 'badge badge-high';
    if (v.includes("o'rta") || v.includes('orta') || v.includes('medium') || v.includes('qoniqarli')) return 'badge badge-mid';
    return 'badge badge-low';
  }

  function parseIfut(value) {
    const raw = (value || '').trim();
    if (!raw || raw === '-') return { code: '-', name: '-' };
    const idx = raw.indexOf(' - ');
    if (idx === -1) return { code: raw, name: '-' };
    return { code: raw.slice(0, idx).trim(), name: raw.slice(idx + 3).trim() || '-' };
  }

  async function fetchCompaniesFromApi() {
    const q = (document.getElementById('filterQuery')?.value || '').trim();
    const region = (document.getElementById('filterRegion')?.value || '').trim();
    const sector = (document.getElementById('filterSector')?.value || '').trim();
    const stability = (document.getElementById('filterStability')?.value || '').trim();
    const legalForm = (document.getElementById('filterLegalForm')?.value || '').trim();
    const thsht = (document.getElementById('filterThsht')?.value || '').trim();
    const params = new URLSearchParams();
    if (q) params.set('q', q);
    if (region) params.set('region', region);
    if (sector) params.set('sector', sector);
    if (stability) params.set('stability', stability);
    if (legalForm) params.set('legal_form', legalForm);
    if (thsht) params.set('thsht', thsht);
    params.set('limit', '200');
    const res = await fetch(`/api/companies?${params.toString()}`);
    if (!res.ok) throw new Error('companies api unavailable');
    const data = await res.json();
    return data.items || [];
  }

  async function renderCompaniesTable(lang) {
    const body = document.getElementById('companiesBody');
    if (!body) return;

    const q = (document.getElementById('filterQuery')?.value || '').toLowerCase().trim();
    const region = (document.getElementById('filterRegion')?.value || '').trim();
    const sector = (document.getElementById('filterSector')?.value || '').trim();

    let filtered = [];
    try {
      const apiItems = await fetchCompaniesFromApi();
      filtered = apiItems.map((x) => ({
        id: x.id,
        name: x.company_name,
        inn: x.inn,
        region: x.region,
        okedCode: x.oked_code || parseIfut(x.ifut).code,
        okedName: x.oked_name || parseIfut(x.ifut).name,
        stability: x.stability_rating || x.category || '-',
        legalForm: x.legal_form || '-',
        thshtCode: x.thsht_code || '-',
        thshtText: x.thsht_text || '-',
        status: x.activity_status,
        score: x.score,
      }));
    } catch (_e) {
      filtered = companies.filter((item) => {
        const matchQ = !q || item.name.toLowerCase().includes(q) || item.inn.includes(q);
        const matchRegion = !region || item.region === region;
        const matchSector = !sector || item.okedCode === sector || (item.okedCode || '').startsWith(sector);
        return matchQ && matchRegion && matchSector;
      });
    }

    body.innerHTML = filtered
      .map(
        (item) => `
          <tr>
            <td>${item.name}</td>
            <td><span title="${item.thshtCode}">${item.thshtText}</span></td>
            <td>${item.inn}</td>
            <td>${item.region}</td>
            <td><span title="${item.okedName}">${item.okedCode}</span></td>
            <td>${item.legalForm}</td>
            <td>${item.status}</td>
            <td><span class="${stabilityBadge(item.stability)}">${item.stability}</span></td>
            <td><span class="${scoreBadge(item.score)}">${item.score}</span></td>
            <td><a class="btn btn-soft" href="./company.html?id=${item.id}">${t(lang, 'action_open')}</a></td>
          </tr>
        `
      )
      .join('');
  }

  async function loadCompaniesMeta() {
    const regionSelect = document.getElementById('filterRegion');
    const sectorSelect = document.getElementById('filterSector');
    const stabilitySelect = document.getElementById('filterStability');
    const legalFormSelect = document.getElementById('filterLegalForm');
    const thshtSelect = document.getElementById('filterThsht');
    if (!regionSelect || !sectorSelect || !stabilitySelect || !legalFormSelect || !thshtSelect) return;

    const res = await fetch('/api/meta/filters');
    if (!res.ok) throw new Error('meta filters failed');
    const data = await res.json();

    regionSelect.innerHTML = `<option value="">${t(getLang(), 'filter_region')}</option>` +
      (data.regions || []).map((r) => `<option value="${r}">${r}</option>`).join('');

    stabilitySelect.innerHTML = `<option value="">${t(getLang(), 'filter_stability')}</option>` +
      (data.stabilities || []).map((s) => `<option value="${s}">${s}</option>`).join('');
    legalFormSelect.innerHTML = `<option value="">${t(getLang(), 'filter_legal_form')}</option>` +
      (data.legal_forms || []).map((s) => `<option value="${s}">${s}</option>`).join('');
    thshtSelect.innerHTML = `<option value="">${t(getLang(), 'filter_thsht')}</option>` +
      (data.thsht_items || []).map((x) => `<option value="${x.thsht}" title="${x.thsht}">${x.thsht}</option>`).join('');

    let sectorHtml = `<option value="">${t(getLang(), 'filter_sector')}</option>`;
    (data.oked_groups || []).forEach((g) => {
      const label = `${g.major_code}xx (${g.count})`;
      sectorHtml += `<optgroup label="${label}">`;
      (g.subcategories || []).forEach((sub) => {
        sectorHtml += `<option value="${sub.code}" title="${sub.name}">${sub.code} — ${sub.name}</option>`;
      });
      sectorHtml += `</optgroup>`;
    });
    sectorSelect.innerHTML = sectorHtml;
  }

  async function renderRfqList() {
    const list = document.getElementById('rfqList');
    if (!list) return;

    let source = rfqSeed;
    try {
      const res = await fetch('/api/rfq?limit=100');
      if (res.ok) {
        const data = await res.json();
        source = (data.items || []).map((x) => ({
          title: x.title,
          stage: x.status || 'OPEN',
          budget: `${x.budget_uzs || 0} UZS`,
        }));
      }
    } catch (_e) {
      // fallback on local seed
    }

    list.innerHTML = source
      .map(
        (rfq) => `
          <div class="timeline-item">
            <p><strong>${rfq.title}</strong></p>
            <p>${rfq.stage} • ${rfq.budget}</p>
          </div>
        `
      )
      .join('');
  }

  function setupCompaniesFilters() {
    const applyBtn = document.getElementById('applyFilters');
    if (!applyBtn) return;

    applyBtn.addEventListener('click', () => renderCompaniesTable(getLang()));

    const params = new URLSearchParams(window.location.search);
    const q = params.get('q') || '';
    if (q && document.getElementById('filterQuery')) {
      document.getElementById('filterQuery').value = q;
      renderCompaniesTable(getLang());
    }
    loadCompaniesMeta()
      .then(() => renderCompaniesTable(getLang()))
      .catch(() => renderCompaniesTable(getLang()));
  }

  function metricCard(label, value) {
    return `<article class="metric"><p class="label">${label}</p><p class="value">${value}</p></article>`;
  }

  async function renderCompanyDetailPage() {
    const titleNode = document.getElementById('companyTitle');
    if (!titleNode) return;

    const params = new URLSearchParams(window.location.search);
    const id = params.get('id');
    if (!id) {
      titleNode.textContent = 'Company not found';
      return;
    }

    const res = await fetch(`/api/companies/${encodeURIComponent(id)}`);
    if (!res.ok) {
      titleNode.textContent = 'Company not found';
      return;
    }
    const data = await res.json();
    const c = data.company || {};

    titleNode.textContent = c.company_name || 'Company';
    const subtitle = document.getElementById('companySubtitle');
    if (subtitle) subtitle.textContent = `INN: ${c.inn || '-'} • Region: ${c.region || '-'} • OKED: ${c.oked_code || '-'}`;

    const stats = document.getElementById('companyStats');
    if (stats) {
      stats.innerHTML = [
        metricCard('Score', c.score || 0),
        metricCard('Stability', c.stability_rating || '-'),
        metricCard('Activity', c.activity_status || '-'),
      ].join('');
    }

    const fields = [
      ['INN', c.inn],
      ['Company Name', c.company_name],
      ['Short Name', c.short_name],
      ['Registration Date', c.registration_date],
      ['Activity Status', c.activity_status],
      ['Stability Rating', c.stability_rating],
      ['Legal Form', c.legal_form],
      ['OKED Code', c.oked_code],
      ['OKED Name', c.oked_name],
      ['Registration Authority', c.registration_authority],
      ['THSHT', c.thsht],
      ['THSHT Text', c.thsht_text],
      ['DBIBT', c.dbibt],
      ['IFUT', c.ifut],
      ['Charter Capital UZS', c.charter_capital_uzs],
      ['Phone', c.phone],
      ['Email', c.email],
      ['Address', c.address],
      ['Region', c.region],
      ['District', c.district],
      ['Director', c.director],
      ['Founders', c.founders],
      ['Tax Committee', c.tax_committee],
      ['Large Taxpayer', c.large_taxpayer],
      ['Source URL', c.source_url],
    ];

    const body = document.getElementById('companyFields');
    if (body) {
      body.innerHTML = fields
        .map(([k, v]) => `<tr><th>${k}</th><td>${v || '-'}</td></tr>`)
        .join('');
    }

    const relBody = document.getElementById('companyRelations');
    if (relBody) {
      const rel = data.relations || [];
      relBody.innerHTML = rel.length
        ? rel.map((x) => `<tr><td>${x.relation_type}</td><td>${x.weight}</td><td>${x.related_company}</td></tr>`).join('')
        : '<tr><td colspan="3">No relations yet</td></tr>';
    }
  }

  function setupRfqPrefill() {
    if (!document.getElementById('rfqForm')) return;
    const payloadRaw = localStorage.getItem('makebiz_rfq_prefill');
    if (!payloadRaw) return;

    try {
      const payload = JSON.parse(payloadRaw);
      if (document.getElementById('rfqNeed') && payload.title) {
        document.getElementById('rfqNeed').value = payload.title;
      }
      if (document.getElementById('rfqBudget') && payload.budget) {
        document.getElementById('rfqBudget').value = payload.budget;
      }
    } catch (_e) {
      // ignore
    }
  }

  async function postRfqToApi(payload) {
    const res = await fetch('/api/rfq', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error('rfq api unavailable');
    return res.json();
  }

  function setupRfqForm() {
    const form = document.getElementById('rfqForm');
    if (!form) return;

    setupRfqPrefill();

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const title = (document.getElementById('rfqNeed')?.value || '').trim();
      const budget = (document.getElementById('rfqBudget')?.value || '').trim() || '0';
      const companyName = (document.getElementById('rfqCompany')?.value || '').trim() || 'MakeBiz User';
      const deadline = (document.getElementById('rfqDeadline')?.value || '').trim() || '-';
      const details = (document.getElementById('rfqDetails')?.value || '').trim() || '-';
      if (!title) return;

      try {
        await postRfqToApi({
          title,
          company_name: companyName,
          budget_uzs: Number(budget.replace(/\D/g, '')) || 0,
          deadline,
          details,
        });
      } catch (_e) {
        rfqSeed.unshift({ title, stage: 'New', budget: `${budget} UZS` });
      }

      form.reset();
      renderRfqList();
      localStorage.removeItem('makebiz_rfq_prefill');
    });
  }

  function setupHeroSearch() {
    const input = document.getElementById('heroSearchInput');
    const btn = document.getElementById('heroSearchBtn');
    if (!input || !btn) return;

    const go = () => {
      const q = input.value.trim();
      const url = q ? `./companies.html?q=${encodeURIComponent(q)}` : './companies.html';
      window.location.href = url;
    };

    btn.addEventListener('click', go);
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') go();
    });
  }

  function setupHotRfqLinks() {
    document.querySelectorAll('.rfq-link').forEach((btn) => {
      btn.addEventListener('click', () => {
        const payload = {
          title: btn.getAttribute('data-rfq-title') || '',
          budget: btn.getAttribute('data-rfq-budget') || '',
        };
        localStorage.setItem('makebiz_rfq_prefill', JSON.stringify(payload));
        window.location.href = './rfq.html';
      });
    });
  }

  function renderCategoriesExplorer() {
    const list = document.getElementById('categoryList');
    const sub = document.getElementById('subcategoryList');
    if (!list || !sub) return;

    function drawSubs(cat) {
      sub.innerHTML = cat.subs
        .map((name) => `<button class="chip" type="button" onclick="window.location.href='./companies.html?q=${encodeURIComponent(name)}'">${name}</button>`)
        .join('');
    }

    list.innerHTML = categoriesTree
      .map(
        (cat, i) => `
          <article class="category-item">
            <button type="button" data-cat-index="${i}">
              <span>${cat.name}</span>
              <span>${cat.subs.length}</span>
            </button>
          </article>
        `
      )
      .join('');

    list.querySelectorAll('button[data-cat-index]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const idx = Number(btn.getAttribute('data-cat-index'));
        drawSubs(categoriesTree[idx]);
      });
    });

    drawSubs(categoriesTree[0]);
  }

  function setupLanguageSwitch() {
    document.querySelectorAll('.lang').forEach((select) => {
      select.addEventListener('change', (e) => setLang(e.target.value));
    });
  }

  function aiAnswer(text, lang) {
    const q = text.toLowerCase();
    if (q.includes('score')) {
      if (lang === 'uz') return 'Score modeli profil to\'liqligi, faoliyat va aloqa signaliga asoslanadi. 75+ kompaniyalar ro\'yxatini bera olaman.';
      if (lang === 'en') return 'Scoring is based on profile completeness, activity and contact quality. I can provide a shortlist with score 75+.';
      return 'Scoring формируется по полноте профиля, активности и контактным сигналам. Могу дать shortlist компаний со score 75+.';
    }
    if (q.includes('rfq')) {
      if (lang === 'uz') return 'RFQ uchun talab, byudjet va muddatni kiriting. Men strukturani tayyorlayman.';
      if (lang === 'en') return 'For RFQ, provide requirement, budget and deadline. I will structure the draft.';
      return 'Для RFQ укажи потребность, бюджет и дедлайн. Я соберу структурированный черновик.';
    }
    if (lang === 'uz') return 'Bu MVP AI yordamchisi. Keyingi bosqichda real data/API ulanishi bo\'ladi.';
    if (lang === 'en') return 'This is MVP AI assistant. Next step is real data/API integration.';
    return 'Это MVP AI-ассистент. Следующий шаг: интеграция реальной базы/API.';
  }

  function addAiMessage(text, who) {
    const box = document.getElementById('aiMessages');
    if (!box) return;
    const item = document.createElement('div');
    item.className = `ai-msg ${who === 'user' ? 'ai-msg-user' : 'ai-msg-bot'}`;
    item.textContent = text;
    box.appendChild(item);
    box.scrollTop = box.scrollHeight;
  }

  function ensureAiIntro(lang) {
    const box = document.getElementById('aiMessages');
    if (!box || box.childElementCount > 0) return;
    addAiMessage(t(lang, 'ai_intro'), 'bot');
  }

  function setupAiFab() {
    const fab = document.getElementById('aiFab');
    const panel = document.getElementById('aiPanel');
    const form = document.getElementById('aiForm');
    const input = document.getElementById('aiInput');
    if (!fab || !panel || !form || !input) return;

    fab.addEventListener('click', () => panel.classList.toggle('active'));

    form.addEventListener('submit', (e) => {
      e.preventDefault();
      const text = input.value.trim();
      if (!text) return;
      addAiMessage(text, 'user');
      input.value = '';
      setTimeout(() => addAiMessage(aiAnswer(text, getLang()), 'bot'), 220);
    });
  }

  function ensureLoginModal() {
    if (document.getElementById('loginModalBackdrop')) return;
    const lang = getLang();

    const backdrop = document.createElement('div');
    backdrop.id = 'loginModalBackdrop';
    backdrop.className = 'modal-backdrop';
    backdrop.innerHTML = `
      <div class="modal">
        <h3 id="loginTitle">${t(lang, 'login_title')}</h3>
        <p id="loginText">${t(lang, 'login_text')}</p>
        <form id="loginForm" class="modal-grid">
          <input id="loginEmail" class="input" type="email" placeholder="${t(lang, 'login_email')}" required>
          <input id="loginPassword" class="input" type="password" placeholder="${t(lang, 'login_password')}" required>
          <div class="modal-actions">
            <button id="loginCancel" class="btn btn-soft" type="button">${t(lang, 'login_cancel')}</button>
            <button class="btn btn-primary" type="submit">${t(lang, 'login_submit')}</button>
          </div>
        </form>
      </div>
    `;
    document.body.appendChild(backdrop);

    const close = () => backdrop.classList.remove('active');
    backdrop.addEventListener('click', (e) => { if (e.target === backdrop) close(); });
    backdrop.querySelector('#loginCancel').addEventListener('click', close);
    backdrop.querySelector('#loginForm').addEventListener('submit', (e) => {
      e.preventDefault();
      const email = (backdrop.querySelector('#loginEmail').value || '').trim();
      localStorage.setItem('makebiz_user_email', email || 'user@makebiz.uz');
      close();
      refreshLoginButtons();
    });
  }

  function refreshLoginButtons() {
    const user = localStorage.getItem('makebiz_user_email');
    document.querySelectorAll('.login-btn').forEach((btn) => {
      btn.textContent = user ? user.split('@')[0] : t(getLang(), 'nav_login');
    });
  }

  function setupLoginButtons() {
    ensureLoginModal();
    const backdrop = document.getElementById('loginModalBackdrop');

    document.querySelectorAll('.login-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        const lang = getLang();
        backdrop.querySelector('#loginTitle').textContent = t(lang, 'login_title');
        backdrop.querySelector('#loginText').textContent = t(lang, 'login_text');
        backdrop.querySelector('#loginEmail').placeholder = t(lang, 'login_email');
        backdrop.querySelector('#loginPassword').placeholder = t(lang, 'login_password');
        backdrop.querySelector('#loginCancel').textContent = t(lang, 'login_cancel');
        backdrop.querySelector('button[type="submit"]').textContent = t(lang, 'login_submit');
        backdrop.classList.add('active');
      });
    });

    refreshLoginButtons();
  }

  function applyI18n(lang) {
    document.querySelectorAll('[data-i18n]').forEach((el) => {
      const key = el.getAttribute('data-i18n');
      el.textContent = t(lang, key);
    });

    document.querySelectorAll('[data-i18n-placeholder]').forEach((el) => {
      const key = el.getAttribute('data-i18n-placeholder');
      el.setAttribute('placeholder', t(lang, key));
    });

    document.querySelectorAll('.lang').forEach((select) => {
      select.value = lang;
    });

    renderCompaniesTable(lang);
    renderRfqList();
    ensureAiIntro(lang);
    refreshLoginButtons();
  }

  function init() {
    setupContainerWidthControl();
    setupLanguageSwitch();
    setupCompaniesFilters();
    setupRfqForm();
    setupHeroSearch();
    setupHotRfqLinks();
    renderCategoriesExplorer();
    setupAiFab();
    setupLoginButtons();
    applyI18n(getLang());
    renderCompanyDetailPage().catch(() => null);
  }

  document.addEventListener('DOMContentLoaded', init);
})();
