/**
 * 主入口 — 初始化 UI、事件绑定、状态管理
 */
import { EmailAPI, ConfigAPI, DbAPI, healthCheck, getApiBase } from './api.js';
import { state, setState, subscribe }                           from './state.js';
import { renderAttachment }                                     from './components/attachment-viewer.js';
import { openForwardDialog, openDraftDialog, openReplyAllDialog } from './components/forward-dialog.js';
import { openSearchDialog }                                     from './components/search-panel.js';

// ── 日志（供其他模块 import） ─────────────────────────────────

export function log(msg, level = 'info') {
  const logArea = document.getElementById('logArea');
  const ts = new Date().toLocaleTimeString('zh-CN', { hour12: false });
  const line = document.createElement('div');
  line.className = level === 'error' ? 'log-error' : level === 'warn' ? 'log-warn' : '';
  line.textContent = `[${ts}] ${msg}`;
  logArea.appendChild(line);
  logArea.scrollTop = logArea.scrollHeight;
  // 保留最近 200 条
  while (logArea.children.length > 200) logArea.removeChild(logArea.firstChild);
}

// ── 启动 ──────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
  _checkConfig();
  await _loadConfig();

  // 事件绑定
  document.getElementById('btnFetch').addEventListener('click', fetchUnread);
  document.getElementById('btnRefresh').addEventListener('click', fetchUnread);
  document.getElementById('btnSearch').addEventListener('click', () => openSearchDialog(_onSearchSelect));
  document.getElementById('btnMarkRead').addEventListener('click', _markSelectedRead);
  document.getElementById('btnSettings').addEventListener('click', _openSettings);
  document.getElementById('chkAll').addEventListener('change', _onChkAll);
  document.getElementById('btnForwardOriginal').addEventListener('click', _onForwardOriginal);
  document.getElementById('btnForwardDraft').addEventListener('click', _onForwardDraft);
  document.getElementById('btnReplyAll').addEventListener('click', _onReplyAll);
  document.getElementById('btnParseEmail').addEventListener('click', _onParseEmail);
  document.getElementById('btnPreviewAtt').addEventListener('click', _onPreviewAtt);
  document.getElementById('btnDownloadAtt').addEventListener('click', _onDownloadAtt);

  // 订阅状态
  subscribe('emails', _renderEmailList);
  subscribe('selectedEmail', _renderEmailDetail);
});

// ── 配置检测 ─────────────────────────────────────────────────

function _checkConfig() {
  const base  = localStorage.getItem('api_base_url');
  const token = localStorage.getItem('api_token');
  if (!base || !token) {
    _openSettings(true);
  } else {
    document.getElementById('emailAddr').textContent = localStorage.getItem('email_addr') || '';
    _pingServer();
  }
}

async function _pingServer() {
  const indicator = document.getElementById('connIndicator');
  try {
    const ok = await healthCheck();
    if (ok) {
      indicator.textContent = '● 已连接';
      indicator.className = 'conn-indicator connected';
      log('后端连接正常');
    } else {
      throw new Error('服务不可用');
    }
  } catch (e) {
    indicator.textContent = '● 连接失败';
    indicator.className = 'conn-indicator error';
    log('后端连接失败：' + e.message, 'error');
  }
}

// ── 加载配置（清关公司、模板、默认值） ───────────────────────

async function _loadConfig() {
  if (!getApiBase()) return;
  try {
    const [brokers, templates, defaults] = await Promise.all([
      ConfigAPI.getBrokers(),
      ConfigAPI.getTemplates(),
      ConfigAPI.getDefaults(),
    ]);
    setState('brokers', brokers);
    setState('templates', templates);
    setState('defaults', defaults);
    log(`已加载 ${Object.keys(brokers).length} 家清关公司，${templates.length} 个模板`);
  } catch (e) {
    log('加载配置失败：' + e.message, 'warn');
  }
}

// ── 获取未读邮件 ─────────────────────────────────────────────

export async function fetchUnread() {
  if (!getApiBase()) { _openSettings(); return; }
  const btn = document.getElementById('btnFetch');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>获取中…';
  log('正在获取未读邮件…');

  try {
    const emails = await EmailAPI.getUnread();
    setState('emails', emails);
    setState('selectedEmail', null);
    setState('checkedIds', new Set());
    log(`获取到 ${emails.length} 封未读邮件`);
    document.getElementById('connIndicator').textContent = '● 已连接';
    document.getElementById('connIndicator').className = 'conn-indicator connected';
  } catch (e) {
    log('获取失败：' + e.message, 'error');
    document.getElementById('connIndicator').textContent = '● 连接失败';
    document.getElementById('connIndicator').className = 'conn-indicator error';
  } finally {
    btn.disabled = false;
    btn.textContent = '获取未读邮件';
  }
}

// ── 渲染邮件列表 ─────────────────────────────────────────────

function _renderEmailList(emails) {
  const listEl = document.getElementById('emailList');
  if (!emails.length) {
    listEl.innerHTML = '<div class="empty-hint">没有未读邮件</div>';
    return;
  }

  listEl.innerHTML = emails.map((e, i) => {
    const attIcon = e.attachments.length ? `📎${e.attachments.length}` : '';
    const from = _shortAddr(e.from_addr);
    const date = e.date.slice(5); // MM-DD HH:MM
    return `
      <div class="email-row unread" data-idx="${i}" data-id="${e.id}">
        <input type="checkbox" class="email-chk" data-id="${e.id}" onclick="event.stopPropagation()">
        <span class="col-date">${_esc(date)}</span>
        <span class="col-from" title="${_esc(e.from_addr)}">${_esc(from)}</span>
        <span class="col-subj" title="${_esc(e.subject)}">${_esc(e.subject)}</span>
        <span class="col-att">${attIcon}</span>
      </div>
    `;
  }).join('');

  // 点击行选中邮件
  listEl.querySelectorAll('.email-row').forEach((row, i) => {
    row.addEventListener('click', () => _selectEmail(i));
  });

  // 复选框联动
  listEl.querySelectorAll('.email-chk').forEach(chk => {
    chk.addEventListener('change', () => {
      const id = chk.dataset.id;
      const newSet = new Set(state.checkedIds);
      chk.checked ? newSet.add(id) : newSet.delete(id);
      setState('checkedIds', newSet);
    });
  });
}

async function _selectEmail(idx) {
  const meta = state.emails[idx];
  if (!meta) return;

  // 高亮选中行
  document.querySelectorAll('.email-row').forEach((r, i) => r.classList.toggle('selected', i === idx));

  // 拉取完整邮件详情
  try {
    const detail = await EmailAPI.getDetail(meta.folder, meta.id);
    setState('selectedEmail', detail);
    _populateAttSelect(detail.attachments);
  } catch (e) {
    log('加载邮件详情失败：' + e.message, 'error');
  }
}

// ── 渲染邮件详情 ─────────────────────────────────────────────

function _renderEmailDetail(email) {
  const metaEl    = document.getElementById('emailMeta');
  const bodyEl    = document.getElementById('emailBody');
  const actionsEl = document.getElementById('detailActions');

  if (!email) {
    metaEl.innerHTML = '<div class="meta-placeholder">← 点击左侧邮件查看详情</div>';
    bodyEl.textContent = '';
    actionsEl.style.display = 'none';
    return;
  }

  metaEl.innerHTML = `
    <div class="meta-subject">${_esc(email.subject)}</div>
    <div class="meta-row"><span class="meta-label">From</span><span class="meta-value">${_esc(email.from_addr)}</span></div>
    <div class="meta-row"><span class="meta-label">To</span><span class="meta-value">${_esc(email.to_addr)}</span></div>
    <div class="meta-row"><span class="meta-label">Date</span><span class="meta-value">${_esc(email.date)}</span></div>
    ${email.attachments.length ? `<div class="meta-row"><span class="meta-label">附件</span><span class="meta-value">${email.attachments.map(a=>_esc(a.filename)).join(' / ')}</span></div>` : ''}
  `;

  bodyEl.textContent = email.body_text || '（无正文）';
  actionsEl.style.display = 'flex';
}

// ── 附件选择器 ────────────────────────────────────────────────

function _populateAttSelect(attachments) {
  const sel = document.getElementById('selectAtt');
  sel.innerHTML = '<option value="">— 选择附件 —</option>';
  (attachments || []).forEach((a, i) => {
    const opt = document.createElement('option');
    opt.value = i;
    opt.textContent = `${a.filename} (${_fmtSize(a.size)})`;
    sel.appendChild(opt);
  });
  document.getElementById('attViewer').innerHTML = '<div class="empty-hint">选择附件后点击「预览」</div>';
}

async function _onPreviewAtt() {
  const sel   = document.getElementById('selectAtt');
  const idx   = parseInt(sel.value);
  const email = state.selectedEmail;
  if (!email || isNaN(idx)) { log('请先选择邮件和附件', 'warn'); return; }

  const viewer = document.getElementById('attViewer');
  viewer.innerHTML = '<div class="empty-hint"><span class="spinner"></span>加载中…</div>';

  try {
    const att = await EmailAPI.getAttachment(email.folder, email.id, idx);
    await renderAttachment(viewer, att);
    log(`已预览：${att.filename}`);
  } catch (e) {
    viewer.innerHTML = `<div class="att-text log-error">加载失败：${e.message}</div>`;
    log('附件加载失败：' + e.message, 'error');
  }
}

async function _onDownloadAtt() {
  const sel   = document.getElementById('selectAtt');
  const idx   = parseInt(sel.value);
  const email = state.selectedEmail;
  if (!email || isNaN(idx)) { log('请先选择邮件和附件', 'warn'); return; }

  try {
    const att  = await EmailAPI.getAttachment(email.folder, email.id, idx);
    const blob = _b64ToBlob(att.data_base64, att.content_type);
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url; a.download = att.filename;
    a.click();
    URL.revokeObjectURL(url);
    log(`已下载：${att.filename}`);
  } catch (e) {
    log('下载失败：' + e.message, 'error');
  }
}

// ── 标记已读 ─────────────────────────────────────────────────

async function _markSelectedRead() {
  const ids = [...state.checkedIds];
  if (!ids.length) { log('请先勾选邮件', 'warn'); return; }
  for (const id of ids) {
    const em = state.emails.find(e => e.id === id);
    if (em) {
      try {
        await EmailAPI.markRead(em.folder, id);
      } catch (e) {
        log(`标记已读失败 ${id}：${e.message}`, 'error');
      }
    }
  }
  log(`已标记 ${ids.length} 封邮件为已读`);
  // 从列表移除
  setState('emails', state.emails.filter(e => !ids.includes(e.id)));
  setState('checkedIds', new Set());
}

// ── 转发操作 ─────────────────────────────────────────────────

function _onForwardOriginal() {
  const email = state.selectedEmail;
  if (!email) { log('请先选择邮件', 'warn'); return; }
  openForwardDialog(email, _onEmailSent);
}

function _onForwardDraft() {
  const email = state.selectedEmail;
  if (!email) { log('请先选择邮件', 'warn'); return; }
  openDraftDialog(email, _onEmailSent);
}

function _onReplyAll() {
  const email = state.selectedEmail;
  if (!email) { log('请先选择邮件', 'warn'); return; }
  openReplyAllDialog(email, _onEmailSent);
}

function _onEmailSent(emailId) {
  // 发送成功后从列表移除
  setState('emails', state.emails.filter(e => e.id !== emailId));
  setState('selectedEmail', null);
}

// ── 解析邮件信息 ─────────────────────────────────────────────

async function _onParseEmail() {
  const email = state.selectedEmail;
  if (!email) { log('请先选择邮件', 'warn'); return; }

  try {
    const parsed = await DbAPI.parseEmail({
      subject: email.subject,
      from_addr: email.from_addr,
      body_text: email.body_text || '',
    });

    const rows = Object.entries(parsed)
      .filter(([k]) => !k.startsWith('_'))
      .map(([k, v]) => `<tr><td>${_esc(k)}</td><td>${_esc(String(v))}</td></tr>`)
      .join('');

    _showInfoModal('🔍 解析结果', `
      <table class="parse-table">${rows}</table>
      <div style="margin-top:10px;display:flex;gap:8px;">
        <button class="btn btn-primary" id="parseInsertBtn">写入数据库</button>
      </div>
    `, parsed);
  } catch (e) {
    log('解析失败：' + e.message, 'error');
  }
}

function _showInfoModal(title, bodyHtml, parsedData) {
  const overlay   = document.getElementById('modalOverlay');
  const container = document.getElementById('modalContainer');

  container.innerHTML = `
    <div class="modal" style="max-width:560px;">
      <div class="modal-header">
        <span>${title}</span>
        <button class="modal-close" id="infoCloseBtn">✕</button>
      </div>
      <div class="modal-body">${bodyHtml}</div>
    </div>
  `;
  overlay.style.display = 'block';

  const modal = container.querySelector('.modal');
  const closeBtn = modal.querySelector('#infoCloseBtn');
  closeBtn.addEventListener('click', () => { overlay.style.display='none'; container.innerHTML=''; });
  overlay.addEventListener('click', () => { overlay.style.display='none'; container.innerHTML=''; }, { once: true });

  const insertBtn = modal.querySelector('#parseInsertBtn');
  if (insertBtn && parsedData) {
    insertBtn.addEventListener('click', async () => {
      insertBtn.disabled = true;
      try {
        const res = await DbAPI.insert(parsedData);
        log(res.message || '已写入数据库');
        overlay.style.display='none'; container.innerHTML='';
      } catch (e) {
        log('写入数据库失败：' + e.message, 'error');
        insertBtn.disabled = false;
      }
    });
  }
}

// ── 搜索结果选中 ─────────────────────────────────────────────

async function _onSearchSelect(emailSummary) {
  try {
    log(`正在加载搜索结果邮件：${emailSummary.subject}`);
    const detail = await EmailAPI.getDetail(emailSummary.folder, emailSummary.id);
    setState('selectedEmail', detail);
    _populateAttSelect(detail.attachments);
  } catch (e) {
    log('加载邮件失败：' + e.message, 'error');
  }
}

// ── 全选 / 取消全选 ──────────────────────────────────────────

function _onChkAll(e) {
  const checked = e.target.checked;
  document.querySelectorAll('.email-chk').forEach(chk => { chk.checked = checked; });
  const newSet = checked ? new Set(state.emails.map(e => e.id)) : new Set();
  setState('checkedIds', newSet);
}

// ── 设置对话框 ────────────────────────────────────────────────

function _openSettings(required = false) {
  const overlay   = document.getElementById('modalOverlay');
  const container = document.getElementById('modalContainer');

  const curBase  = localStorage.getItem('api_base_url') || '';
  const curToken = localStorage.getItem('api_token') || '';
  const curEmail = localStorage.getItem('email_addr') || '';

  container.innerHTML = `
    <div class="modal" style="max-width:460px;">
      <div class="modal-header">
        <span>⚙ API 设置</span>
        ${!required ? '<button class="modal-close" id="setCloseBtn">✕</button>' : ''}
      </div>
      <div class="modal-body">
        <div class="form-group">
          <label class="form-label">后端 API 地址</label>
          <input class="form-input" id="setBase" value="${_esc(curBase)}" placeholder="https://your-app.railway.app">
          <div class="settings-note">Railway 部署后在 Dashboard 查看域名</div>
        </div>
        <div class="form-group">
          <label class="form-label">API Token</label>
          <input class="form-input" id="setToken" type="password" value="${_esc(curToken)}" placeholder="API_TOKEN 环境变量的值">
        </div>
        <div class="form-group">
          <label class="form-label">邮箱地址（仅显示用）</label>
          <input class="form-input" id="setEmail" value="${_esc(curEmail)}" placeholder="your@email.com">
        </div>
        ${required ? '<div class="settings-note" style="color:#e02424;">首次使用请先填写后端地址和 Token</div>' : ''}
      </div>
      <div class="modal-footer">
        <button class="btn btn-primary" id="setSaveBtn">保存并连接</button>
      </div>
    </div>
  `;

  overlay.style.display = 'block';

  const modal = container.querySelector('.modal');
  const closeBtn = modal.querySelector('#setCloseBtn');
  if (closeBtn) {
    closeBtn.addEventListener('click', () => { overlay.style.display='none'; container.innerHTML=''; });
  }

  modal.querySelector('#setSaveBtn').addEventListener('click', async () => {
    const base  = modal.querySelector('#setBase').value.trim().replace(/\/$/, '');
    const token = modal.querySelector('#setToken').value.trim();
    const email = modal.querySelector('#setEmail').value.trim();

    if (!base || !token) { alert('请填写后端地址和 Token'); return; }

    localStorage.setItem('api_base_url', base);
    localStorage.setItem('api_token', token);
    localStorage.setItem('email_addr', email);
    document.getElementById('emailAddr').textContent = email;

    overlay.style.display = 'none';
    container.innerHTML = '';

    await _pingServer();
    await _loadConfig();
  });
}

// ── 工具函数 ──────────────────────────────────────────────────

function _esc(str) {
  return (str || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function _shortAddr(addr) {
  const m = addr.match(/"?([^"<]+)"?\s*</);
  if (m) return m[1].trim().slice(0, 18);
  const plain = addr.replace(/<.*>/, '').trim();
  return plain.slice(0, 18);
}

function _fmtSize(bytes) {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024*1024) return `${(bytes/1024).toFixed(1)}KB`;
  return `${(bytes/1024/1024).toFixed(1)}MB`;
}

function _b64ToBlob(b64, mimeType) {
  const raw = atob(b64);
  const buf = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i++) buf[i] = raw.charCodeAt(i);
  return new Blob([buf], { type: mimeType });
}
