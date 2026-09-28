const testPath = '/user-push-test';

self.addEventListener('install', (event) => event.waitUntil(self.skipWaiting()));
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));

self.addEventListener('push', (event) => {
  let payload = {};
  try {
    payload = event.data?.json() || {};
  } catch {
    payload = { body: event.data?.text() };
  }
  const response = Object.keys(payload).length === 0
    ? { notificationId: String(Date.now()), expiresAt: new Date(Date.now() + 30000).toISOString() }
    : /^\d+$/.test(String(payload.notificationId || '')) && Number.isFinite(Date.parse(payload.expiresAt))
      ? { notificationId: String(payload.notificationId), expiresAt: payload.expiresAt }
      : null;
  const canAnswer = response && Date.parse(response.expiresAt) > Date.now();
  event.waitUntil(Promise.all([
    self.registration.showNotification(payload.title || 'On:마음 안심 알림 · 테스트', {
      body: payload.body || (response ? '생활 신호가 확인되지 않았어요. 지금 상태를 알려주세요.' : '앱을 열어 안내를 확인해 주세요.'),
      icon: '/android-chrome-192x192.png',
      badge: '/favicon-32x32.png',
      tag: `onmaeum-user-push-test-${response?.notificationId || Date.now()}`,
      requireInteraction: true,
      actions: canAnswer ? [
        { action: 'yes', title: '도움이 필요해요' },
        { action: 'no', title: '괜찮아요' },
      ] : [],
      data: { ...response, url: response ? `${testPath}?${new URLSearchParams(response)}` : testPath },
    }),
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
      clients.filter((client) => new URL(client.url).pathname === testPath)
        .forEach((client) => client.postMessage({ type: 'TEST_PUSH_RECEIVED', ...response, title: payload.title, body: payload.body }));
    }),
  ]));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil((async () => {
    const data = event.notification.data || {};
    const target = new URL(data.url || testPath, self.location.origin);
    if (Date.parse(data.expiresAt) > Date.now() && (event.action === 'yes' || event.action === 'no')) {
      target.searchParams.set('answer', event.action);
    }
    const clients = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    const client = clients.find((item) => new URL(item.url).pathname === testPath);
    if (client && 'navigate' in client) {
      const navigated = await client.navigate(target.href);
      if (navigated) { await navigated.focus(); return; }
    }
    await self.clients.openWindow(target.href);
  })());
});
