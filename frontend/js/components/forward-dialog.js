/**
 * 转发正本对话框
 */
import { EmailAPI, DbAPI, ConfigAPI } from '../api.js';
import { state } from '../state.js';
import { log } from '../app.js';

// ── 渠道相关辅助 ─────────────────────────────────────────────

function _getBrokerInfo(brokers, name) {
  const raw = brokers[name];
  if (!raw) return { emails: [], channel: 'email' };
  // 新结构 {emails, channel}；容忍旧结构 [email,...]
  if (Array.isArray(raw)) return { emails: raw, channel: 'email' };
  return { emails: raw.emails || [], channel: raw.channel || 'email' };
}

function _brokerOptionHtml(name, info) {
  const suffix = info.channel === 'wechat' ? ' [微信群]' : '';
  return `<option value="${name}" data-channel="${info.channel}">${name}${suffix}</option>`;
}

function _canShareFiles() {
  return !!(navigator.canShare && navigator.share);
}

export function openForwardDialog(email, onSuccess) {
  const brokers = state.brokers;
  const brokerNames = Object.keys(brokers);
  const defaultBody = state.defaults.forward_body || '';
  const defaultCc   = (state.defaults.cc || []).join(', ');

  const attItems = (email.attachments || []).map((a, i) =>
    `<label class="att-check-item">
      <input type="checkbox" class="att-sel-chk" value="${i}" checked>
      ${a.filename} <span style="color:#6b7280;font-size:11px;">(${_fmtSize(a.size)})</span>
    </label>`
  ).join('');

  const brokerOptions = brokerNames
    .map(n => _brokerOptionHtml(n, _getBrokerInfo(brokers, n)))
    .join('');

  const html = `
    <div class="form-group">
      <div class="meta-subject">${_esc(email.subject)}</div>
      <div style="font-size:12px;color:#6b7280;">
        From: ${_esc(email.from_addr)} &nbsp;|&nbsp; ${_esc(email.date)}
      </div>
    </div>

    <div class="form-group">
      <label class="form-label">转发正文（可编辑）</label>
      <textarea class="form-textarea" id="fwdBody" rows="5">${_esc(defaultBody)}</textarea>
    </div>

    <div class="form-group">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
        <label class="form-label" style="margin:0;">选择附件</label>
        <button type="button" class="btn btn-sm" id="attSelectAll">全选</button>
        <button type="button" class="btn btn-sm" id="attSelectNone">取消</button>
      </div>
      <div class="att-check-list" id="attCheckList">
        ${attItems || '<div style="color:#6b7280;font-size:12px;padding:4px;">此邮件没有附件</div>'}
      </div>
    </div>

    <div class="form-group">
      <label class="form-label">清关公司</label>
      <select class="form-select" id="brokerSelect">
        <option value="">— 选择清关公司 —</option>
        ${brokerOptions}
      </select>
      <div id="brokerRecipients" style="margin-top:6px;font-size:12px;color:#374151;"></div>
    </div>

    <div class="form-group">
      <label class="form-label">抄送（CC）</label>
      <input class="form-input" id="fwdCc" value="${_esc(defaultCc)}" placeholder="逗号分隔多个地址">
    </div>
  `;

  const modal = _createModal('✉ 转发正本', html, [
    { label: '取消', cls: 'btn', onClick: _closeModal },
    {
      label: '发送', cls: 'btn btn-primary', id: 'fwdSendBtn',
      onClick: () => {
        const broker = modal.querySelector('#brokerSelect').value;
        const info = _getBrokerInfo(state.brokers, broker);
        if (info.channel === 'wechat') {
          _doWechatShare(email, onSuccess);
        } else {
          _doForward(email, onSuccess);
        }
      },
    },
  ]);

  // 全选 / 取消
  modal.querySelector('#attSelectAll')?.addEventListener('click', () => {
    modal.querySelectorAll('.att-sel-chk').forEach(c => c.checked = true);
  });
  modal.querySelector('#attSelectNone')?.addEventListener('click', () => {
    modal.querySelectorAll('.att-sel-chk').forEach(c => c.checked = false);
  });

  // 清关公司选择 → 显示渠道 / 邮件地址
  const brokerSel = modal.querySelector('#brokerSelect');
  const recipDiv  = modal.querySelector('#brokerRecipients');
  const sendBtn   = modal.querySelector('#fwdSendBtn');
  const ccInput   = modal.querySelector('#fwdCc');
  brokerSel.addEventListener('change', () => {
    _applyBrokerChannel(brokerSel.value, state.brokers, recipDiv, sendBtn, ccInput);
  });

  // 尝试自动匹配 BL 号
  _tryAutoMatchBroker(email, brokerSel, recipDiv, brokers, sendBtn, ccInput);
}

function _applyBrokerChannel(name, brokers, recipDiv, sendBtn, ccInput) {
  if (!name) {
    recipDiv.innerHTML = '';
    if (sendBtn) sendBtn.textContent = '发送';
    if (ccInput) ccInput.disabled = false;
    return;
  }
  const info = _getBrokerInfo(brokers, name);
  if (info.channel === 'wechat') {
    recipDiv.innerHTML = '📲 <span style="color:#047857;">此清关公司走微信群：点击下方按钮后在系统分享面板选择"微信"</span>';
    if (sendBtn) sendBtn.textContent = '📲 分享到微信';
    if (ccInput) { ccInput.disabled = true; ccInput.value = ''; }
  } else {
    const addrs = info.emails;
    recipDiv.innerHTML = addrs.length ? `收件人：<b>${addrs.join(', ')}</b>` : '';
    if (sendBtn) sendBtn.textContent = '发送';
    if (ccInput) ccInput.disabled = false;
  }
}

async function _tryAutoMatchBroker(email, selectEl, recipDiv, brokers, sendBtn, ccInput) {
  // 从 subject 中提取 BL 号
  const blMatch = email.subject.match(/\b(2\d{8}|SITTAG[A-Z]{2}\d{6,8}|SITG[A-Z]{4}\d{6,8}|OOLU[A-Z0-9]{8,12}|COAU\d{10,13}|MCLP[A-Z0-9]{8,12}|CNHU[A-Z0-9]{8,12}|EGLV[A-Z0-9]{8,12}|HLCU[A-Z0-9]{8,12}|MAEU[A-Z0-9]{8,12})\b/i);
  if (!blMatch) return;
  try {
    const res = await DbAPI.getBrokerByBl(blMatch[0]);
    if (!res.broker) return;

    const name = res.broker;
    const info = _getBrokerInfo(brokers, name);

    // 若下拉列表中尚无此选项（清关公司未在配置中），动态插入
    let hasOpt = false;
    for (const opt of selectEl.options) {
      if (opt.value === name) { hasOpt = true; break; }
    }
    if (!hasOpt) {
      const opt = document.createElement('option');
      opt.value = name;
      opt.textContent = name + (info.channel === 'wechat' ? ' [微信群]' : '');
      opt.dataset.channel = info.channel;
      selectEl.appendChild(opt);
    }

    selectEl.value = name;
    _applyBrokerChannel(name, brokers, recipDiv, sendBtn, ccInput);

    // 在自动匹配的基础上追加 "自动匹配" 角标
    if (info.channel !== 'wechat') {
      const suffix = info.emails.length
        ? ` <span style="color:#6b7280;">(自动匹配)</span>`
        : ` <span style="color:#6b7280;">(自动匹配：${_esc(name)}，请确认邮件地址已配置)</span>`;
      recipDiv.innerHTML = (recipDiv.innerHTML || `<span style="color:#6b7280;">(自动匹配：${_esc(name)})</span>`) + suffix;
    } else {
      recipDiv.innerHTML += ' <span style="color:#6b7280;">(自动匹配)</span>';
    }
  } catch (e) {
    // 静默失败
  }
}

// ── 微信分享（Web Share API） ────────────────────────────────

async function _doWechatShare(email, onSuccess) {
  const modal   = document.querySelector('.modal');
  const sendBtn = modal.querySelector('#fwdSendBtn');
  const broker  = modal.querySelector('#brokerSelect').value;
  const body    = modal.querySelector('#fwdBody')?.value.trim() || '';
  const checked = [...modal.querySelectorAll('.att-sel-chk:checked')].map(c => parseInt(c.value));

  if (!broker) { alert('请选择清关公司'); return; }
  if (!_canShareFiles()) {
    alert('当前浏览器不支持系统级文件分享。请用手机浏览器打开 PWA，或手动下载附件后在微信中发送。');
    return;
  }

  sendBtn.disabled = true;
  sendBtn.innerHTML = '<span class="spinner"></span>准备文件…';

  try {
    const files = [];
    for (const idx of checked) {
      const res = await EmailAPI.getAttachment(email.folder, email.id, idx);
      const bytes = Uint8Array.from(atob(res.data_base64), c => c.charCodeAt(0));
      const type = res.content_type || 'application/octet-stream';
      files.push(new File([bytes], res.filename, { type }));
    }

    const sharePayload = {
      title: email.subject || '',
      text: body || email.subject || '',
    };
    if (files.length) sharePayload.files = files;

    if (files.length && !navigator.canShare(sharePayload)) {
      alert('当前浏览器不允许分享这些文件。请用手机浏览器打开，或手动下载附件后发送。');
      return;
    }

    await navigator.share(sharePayload);
    _closeModal();
    log(`已唤起系统分享到微信（${broker}）`);
    if (onSuccess) onSuccess(email.id);
  } catch (e) {
    // 用户取消分享不视为失败
    if (e && e.name === 'AbortError') {
      log('已取消分享');
    } else {
      log('分享失败：' + (e?.message || e), 'error');
      alert('分享失败：' + (e?.message || e));
    }
  } finally {
    sendBtn.disabled = false;
    sendBtn.textContent = '📲 分享到微信';
  }
}

async function _doForward(email, onSuccess) {
  const modal    = document.querySelector('.modal');
  const sendBtn  = modal.querySelector('#fwdSendBtn');
  const broker   = modal.querySelector('#brokerSelect').value;
  const body     = modal.querySelector('#fwdBody').value.trim();
  const ccRaw    = modal.querySelector('#fwdCc').value;
  const checked  = [...modal.querySelectorAll('.att-sel-chk:checked')].map(c => parseInt(c.value));

  if (!broker) { alert('请选择清关公司'); return; }
  const info = _getBrokerInfo(state.brokers, broker);
  const toAddrs = info.emails;
  if (!toAddrs.length) { alert('清关公司没有邮件地址'); return; }

  const ccAddrs = ccRaw.split(',').map(s => s.trim()).filter(Boolean);

  sendBtn.disabled = true;
  sendBtn.innerHTML = '<span class="spinner"></span>发送中…';

  try {
    const res = await EmailAPI.forward({
      email_id: email.id,
      folder: email.folder,
      broker_name: broker,
      to_addrs: toAddrs,
      cc_addrs: ccAddrs,
      forward_body: body,
      selected_attachment_indices: checked,
    });
    _closeModal();
    log(res.message || '转发成功');
    if (onSuccess) onSuccess(email.id);
  } catch (e) {
    log('转发失败：' + e.message, 'error');
    alert('转发失败：' + e.message);
  } finally {
    sendBtn.disabled = false;
    sendBtn.textContent = '发送';
  }
}

// ── 转发草稿对话框 ────────────────────────────────────────────

export function openDraftDialog(email, onSuccess) {
  const brokers = state.brokers;
  const brokerNames = Object.keys(brokers);
  const templates   = state.templates;

  const brokerOptions = brokerNames
    .map(n => _brokerOptionHtml(n, _getBrokerInfo(brokers, n)))
    .join('');
  const tplButtons = templates.map(t =>
    `<button type="button" class="tpl-btn" data-body="${_esc(t.body)}">
       <span class="tpl-name">${_esc(t.name)}</span>
     </button>`
  ).join('');

  const html = `
    <div style="display:flex;gap:12px;height:340px;">
      <div style="flex:1;display:flex;flex-direction:column;gap:8px;min-width:0;">
        <label class="form-label">邮件正文</label>
        <textarea class="form-textarea" id="draftBody" style="flex:1;resize:none;"></textarea>

        <div class="form-group" style="margin:0;">
          <label class="form-label">清关公司</label>
          <select class="form-select" id="draftBrokerSelect">
            <option value="">— 选择清关公司 —</option>
            ${brokerOptions}
          </select>
          <div id="draftBrokerRecipients" style="margin-top:4px;font-size:12px;color:#374151;"></div>
        </div>
      </div>

      <div style="width:160px;flex-shrink:0;display:flex;flex-direction:column;gap:4px;">
        <div class="form-label">模板</div>
        <div class="tpl-list" id="tplList" style="overflow-y:auto;flex:1;">
          ${tplButtons || '<div style="color:#6b7280;font-size:12px;">暂无模板</div>'}
        </div>
      </div>
    </div>
  `;

  const modal = _createModal('📋 转发草稿', html, [
    { label: '取消', cls: 'btn', onClick: _closeModal },
    {
      label: '发送', cls: 'btn btn-primary', id: 'draftSendBtn',
      onClick: () => _doDraftSend(email, onSuccess),
    },
  ]);

  // 点击模板 → 填入正文
  modal.querySelectorAll('.tpl-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      modal.querySelector('#draftBody').value = btn.dataset.body;
    });
  });

  // 清关公司联动
  const brokerSel = modal.querySelector('#draftBrokerSelect');
  const recipDiv  = modal.querySelector('#draftBrokerRecipients');
  brokerSel.addEventListener('change', () => {
    const name = brokerSel.value;
    const info = _getBrokerInfo(brokers, name);
    if (info.channel === 'wechat') {
      recipDiv.innerHTML = '📲 <span style="color:#047857;">微信群清关公司不支持草稿模式，请用"转发正本"走微信分享</span>';
    } else {
      recipDiv.innerHTML = info.emails.length ? `收件人：<b>${info.emails.join(', ')}</b>` : '';
    }
  });

  _tryAutoMatchBroker(email, brokerSel, recipDiv, brokers, null, null);
}

async function _doDraftSend(email, onSuccess) {
  const modal    = document.querySelector('.modal');
  const sendBtn  = modal.querySelector('#draftSendBtn');
  const broker   = modal.querySelector('#draftBrokerSelect').value;
  const body     = modal.querySelector('#draftBody').value.trim();

  if (!broker) { alert('请选择清关公司'); return; }
  if (!body)   { alert('请填写邮件正文'); return; }
  const info = _getBrokerInfo(state.brokers, broker);
  if (info.channel === 'wechat') {
    alert('微信群清关公司不支持草稿模式，请改用"转发正本"走微信分享');
    return;
  }
  const toAddrs = info.emails;
  if (!toAddrs.length) { alert('清关公司没有邮件地址'); return; }

  sendBtn.disabled = true;
  sendBtn.innerHTML = '<span class="spinner"></span>发送中…';

  try {
    const res = await EmailAPI.forwardDraft({
      email_id: email.id,
      folder: email.folder,
      broker_name: broker,
      to_addrs: toAddrs,
      body_text: body,
    });
    _closeModal();
    log(res.message || '草稿发送成功');
    if (onSuccess) onSuccess(email.id);
  } catch (e) {
    log('草稿发送失败：' + e.message, 'error');
    alert('草稿发送失败：' + e.message);
  } finally {
    sendBtn.disabled = false;
    sendBtn.textContent = '发送';
  }
}

// ── 工具函数 ──────────────────────────────────────────────────

function _esc(str) {
  return (str || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function _fmtSize(bytes) {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes/1024).toFixed(1)}KB`;
  return `${(bytes/1024/1024).toFixed(1)}MB`;
}

function _createModal(title, bodyHtml, buttons) {
  const overlay = document.getElementById('modalOverlay');
  const container = document.getElementById('modalContainer');

  const btnHtml = buttons.map(b =>
    `<button type="button" class="${b.cls}" ${b.id ? `id="${b.id}"` : ''}>${b.label}</button>`
  ).join('');

  container.innerHTML = `
    <div class="modal">
      <div class="modal-header">
        <span>${title}</span>
        <button class="modal-close" id="modalCloseBtn">✕</button>
      </div>
      <div class="modal-body">${bodyHtml}</div>
      <div class="modal-footer">${btnHtml}</div>
    </div>
  `;

  overlay.style.display = 'block';

  const modal = container.querySelector('.modal');

  // 绑定按钮回调
  buttons.forEach(b => {
    const el = b.id ? modal.querySelector(`#${b.id}`) : null;
    if (el) el.addEventListener('click', b.onClick);
  });
  modal.querySelector('#modalCloseBtn').addEventListener('click', _closeModal);
  overlay.addEventListener('click', _closeModal, { once: true });

  return modal;
}

function _closeModal() {
  document.getElementById('modalOverlay').style.display = 'none';
  document.getElementById('modalContainer').innerHTML = '';
  document.getElementById('modalOverlay').replaceWith(
    document.getElementById('modalOverlay').cloneNode(false)
  );
  document.getElementById('modalOverlay').style.display = 'none';
}

// ── 回复全部对话框 ────────────────────────────────────────────

export function openReplyAllDialog(email, onSuccess) {
  const html = `
    <div class="form-group">
      <div class="meta-subject">${_esc(email.subject)}</div>
      <div style="font-size:12px;color:#6b7280;">
        From: ${_esc(email.from_addr)} &nbsp;|&nbsp; ${_esc(email.date)}
      </div>
      <div style="font-size:12px;color:#6b7280;margin-top:2px;">
        To: ${_esc(email.to_addr || '')}
      </div>
    </div>
    <div class="form-group">
      <label class="form-label">回复正文</label>
      <textarea class="form-textarea" id="replyAllBody" rows="6" placeholder="请输入回复内容…"></textarea>
    </div>
  `;

  _createModal('↩ 回复全部', html, [
    { label: '取消', cls: 'btn', onClick: _closeModal },
    {
      label: '发送', cls: 'btn btn-primary', id: 'replyAllSendBtn',
      onClick: () => _doReplyAll(email, onSuccess),
    },
  ]);
}

async function _doReplyAll(email, onSuccess) {
  const modal   = document.querySelector('.modal');
  const sendBtn = modal.querySelector('#replyAllSendBtn');
  const body    = modal.querySelector('#replyAllBody').value.trim();

  if (!body) { alert('请填写回复正文'); return; }

  sendBtn.disabled = true;
  sendBtn.innerHTML = '<span class="spinner"></span>发送中…';

  try {
    const res = await EmailAPI.replyAll({
      email_id: email.id,
      folder: email.folder,
      reply_body: body,
    });
    _closeModal();
    log(res.message || '回复成功');
    if (onSuccess) onSuccess(email.id);
  } catch (e) {
    log('回复失败：' + e.message, 'error');
    alert('回复失败：' + e.message);
  } finally {
    sendBtn.disabled = false;
    sendBtn.textContent = '发送';
  }
}
