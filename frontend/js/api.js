/**
 * API 层 — 所有后端请求封装
 * API_BASE 和 TOKEN 存储在 localStorage
 */

export function getApiBase() {
  return (localStorage.getItem('api_base_url') || '').replace(/\/$/, '');
}

function getToken() {
  return localStorage.getItem('api_token') || '';
}

function headers() {
  return {
    'Authorization': `Bearer ${getToken()}`,
    'Content-Type': 'application/json',
  };
}

async function _fetch(path, options = {}) {
  const url = getApiBase() + path;
  const res = await fetch(url, { ...options, headers: { ...headers(), ...(options.headers || {}) } });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json();
}

// ── 邮件 ──────────────────────────────────────────────────────

export const EmailAPI = {
  getUnread:      ()           => _fetch('/api/emails/unread'),
  getFolders:     ()           => _fetch('/api/emails/folders'),
  getDetail:      (folder, id) => _fetch(`/api/emails/${encodeURIComponent(folder)}/${id}`),
  getAttachment:  (folder, id, idx) =>
    _fetch(`/api/emails/${encodeURIComponent(folder)}/${id}/attachments/${idx}`),
  search:         (req)        => _fetch('/api/emails/search', { method: 'POST', body: JSON.stringify(req) }),
  markRead:       (folder, id) => _fetch(`/api/emails/${encodeURIComponent(folder)}/${id}/mark-read`, { method: 'POST' }),
  forward:        (req)        => _fetch('/api/emails/forward', { method: 'POST', body: JSON.stringify(req) }),
  forwardDraft:   (req)        => _fetch('/api/emails/forward-draft', { method: 'POST', body: JSON.stringify(req) }),
  replyAll:       (req)        => _fetch('/api/emails/reply-all', { method: 'POST', body: JSON.stringify(req) }),
};

// ── 数据库 ────────────────────────────────────────────────────

export const DbAPI = {
  insert:          (parsed)    => _fetch('/api/db/insert', { method: 'POST', body: JSON.stringify({ parsed }) }),
  updateProgress:  (bl, prog)  => _fetch('/api/db/progress', { method: 'PUT', body: JSON.stringify({ bl_no: bl, progress_value: prog }) }),
  getBrokerByBl:   (bl)        => _fetch(`/api/db/broker/${encodeURIComponent(bl)}`),
  parseEmail:      (data)      => _fetch('/api/db/parse', { method: 'POST', body: JSON.stringify({ parsed: data }) }),
};

// ── 配置 ──────────────────────────────────────────────────────

export const ConfigAPI = {
  getBrokers:      ()           => _fetch('/api/config/brokers'),
  createBroker:    (req)        => _fetch('/api/config/brokers', { method: 'POST', body: JSON.stringify(req) }),
  updateBroker:    (name, req)  => _fetch(`/api/config/brokers/${encodeURIComponent(name)}`, { method: 'PUT', body: JSON.stringify(req) }),
  deleteBroker:    (name)       => _fetch(`/api/config/brokers/${encodeURIComponent(name)}`, { method: 'DELETE' }),
  getTemplates:    ()           => _fetch('/api/config/templates'),
  createTemplate:  (req)        => _fetch('/api/config/templates', { method: 'POST', body: JSON.stringify(req) }),
  updateTemplate:  (name, req)  => _fetch(`/api/config/templates/${encodeURIComponent(name)}`, { method: 'PUT', body: JSON.stringify(req) }),
  deleteTemplate:  (name)       => _fetch(`/api/config/templates/${encodeURIComponent(name)}`, { method: 'DELETE' }),
  getDefaults:     ()           => _fetch('/api/config/defaults'),
};

// ── 健康检查 ─────────────────────────────────────────────────

export async function healthCheck() {
  const res = await fetch(getApiBase() + '/health');
  return res.ok;
}
