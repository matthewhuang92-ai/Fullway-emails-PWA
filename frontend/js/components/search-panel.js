/**
 * 邮件搜索对话框
 */
import { EmailAPI } from '../api.js';
import { log } from '../app.js';

export function openSearchDialog(onSelect) {
  const html = `
    <div class="form-group" style="display:flex;gap:8px;flex-wrap:wrap;">
      <input class="form-input" id="srKeyword" placeholder="关键词" style="flex:1;min-width:120px;">
      <select class="form-select" id="srScope" style="width:110px;">
        <option value="all">全部字段</option>
        <option value="subject">主题</option>
        <option value="from">发件人</option>
        <option value="body">正文</option>
      </select>
      <button class="btn btn-primary" id="srBtn">搜索</button>
    </div>
    <div class="form-group" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
      <label class="form-label" style="margin:0;white-space:nowrap;">日期范围</label>
      <input class="form-input" id="srDateFrom" type="date" style="width:140px;">
      <span>—</span>
      <input class="form-input" id="srDateTo" type="date" style="width:140px;">
    </div>
    <div id="srResults" class="search-results"></div>
  `;

  const overlay   = document.getElementById('modalOverlay');
  const container = document.getElementById('modalContainer');

  container.innerHTML = `
    <div class="modal" style="max-width:600px;">
      <div class="modal-header">
        <span>🔍 搜索邮件</span>
        <button class="modal-close" id="srCloseBtn">✕</button>
      </div>
      <div class="modal-body">${html}</div>
    </div>
  `;
  overlay.style.display = 'block';

  const modal = container.querySelector('.modal');
  modal.querySelector('#srCloseBtn').addEventListener('click', _close);
  overlay.addEventListener('click', _close, { once: true });

  modal.querySelector('#srBtn').addEventListener('click', async () => {
    const keyword  = modal.querySelector('#srKeyword').value.trim();
    const scope    = modal.querySelector('#srScope').value;
    const dateFrom = modal.querySelector('#srDateFrom').value || null;
    const dateTo   = modal.querySelector('#srDateTo').value || null;
    const resDiv   = modal.querySelector('#srResults');

    resDiv.innerHTML = '<div class="empty-hint"><span class="spinner"></span>搜索中…</div>';

    try {
      const results = await EmailAPI.search({
        keyword, search_in: scope,
        folders: ['INBOX', 'Sent Messages'],
        date_from: dateFrom, date_to: dateTo,
        max_results: 50,
      });

      if (!results.length) {
        resDiv.innerHTML = '<div class="empty-hint">没有找到匹配的邮件</div>';
        return;
      }

      resDiv.innerHTML = results.map((e, i) => `
        <div class="search-result-item" data-idx="${i}">
          <div class="sr-subject">${_esc(e.subject)}</div>
          <div class="sr-meta">${_esc(e.from_addr)} &nbsp;|&nbsp; ${_esc(e.date)} &nbsp;|&nbsp; ${_esc(e.folder)}</div>
        </div>
      `).join('');

      resDiv.querySelectorAll('.search-result-item').forEach((el, i) => {
        el.addEventListener('click', () => {
          _close();
          if (onSelect) onSelect(results[i]);
        });
      });
    } catch (err) {
      resDiv.innerHTML = `<div class="empty-hint log-error">搜索失败：${err.message}</div>`;
      log('搜索失败：' + err.message, 'error');
    }
  });

  // 回车触发搜索
  modal.querySelector('#srKeyword').addEventListener('keydown', e => {
    if (e.key === 'Enter') modal.querySelector('#srBtn').click();
  });
}

function _close() {
  document.getElementById('modalOverlay').style.display = 'none';
  document.getElementById('modalContainer').innerHTML = '';
}

function _esc(str) {
  return (str || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
