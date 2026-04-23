/**
 * 轻量状态管理（发布/订阅模式，无框架）
 */

export const state = {
  emails: [],           // 邮件列表
  selectedEmail: null,  // 当前选中邮件（完整对象）
  checkedIds: new Set(),// 勾选的邮件 id
  brokers: {},          // { name: { emails: [email,...], channel: 'email'|'wechat' } }
  templates: [],        // [{ name, body }]
  defaults: {},         // { cc: [], forward_body: "" }
  loading: false,
};

const _subs = {};

export function subscribe(key, fn) {
  if (!_subs[key]) _subs[key] = [];
  _subs[key].push(fn);
}

export function setState(key, value) {
  state[key] = value;
  (_subs[key] || []).forEach(fn => fn(value));
}
