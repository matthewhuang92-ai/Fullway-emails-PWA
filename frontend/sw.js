/**
 * Service Worker — 缓存策略
 * - 静态资源（HTML/CSS/JS）：Cache First
 * - API 请求：Network Only（不缓存，保证数据实时）
 */

const CACHE_NAME = 'fullway-email-v1';

const STATIC_ASSETS = [
  './',
  './index.html',
  './css/style.css',
  './js/app.js',
  './js/api.js',
  './js/state.js',
  './js/components/attachment-viewer.js',
  './js/components/forward-dialog.js',
  './js/components/search-panel.js',
  './manifest.json',
];

// 安装：预缓存静态资源
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(STATIC_ASSETS))
  );
  self.skipWaiting();
});

// 激活：清理旧缓存
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// 拦截请求
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // API 请求：直接走网络，不缓存
  if (url.pathname.startsWith('/api/') || url.pathname === '/health') {
    return; // 不 event.respondWith，让浏览器正常请求
  }

  // CDN 第三方资源：Network First，失败则 Cache
  if (url.hostname !== self.location.hostname) {
    event.respondWith(
      fetch(event.request).catch(() => caches.match(event.request))
    );
    return;
  }

  // 本地静态资源：Cache First
  event.respondWith(
    caches.match(event.request).then(cached => {
      if (cached) return cached;
      return fetch(event.request).then(res => {
        const clone = res.clone();
        caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        return res;
      });
    })
  );
});
