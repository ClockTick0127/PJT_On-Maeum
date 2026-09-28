function parsePayload(event) {
  try { return event.data?.json() || {} }
  catch { return { title: 'On:마음 알림', body: event.data?.text() } }
}

function responseNotification(data) {
  const notificationId = String(data.notificationId || '')
  if (!/^\d+$/.test(notificationId) || typeof data.expiresAt !== 'string' || !Number.isFinite(Date.parse(data.expiresAt))) return null
  return { notificationId, expiresAt: data.expiresAt }
}

function notificationUrl(data) {
  const response = responseNotification(data)
  if (response) {
    return `/user?${new URLSearchParams(response)}`
  }
  try {
    const url = new URL(data.url || '/user', self.location.origin)
    url.searchParams.delete('notificationId')
    url.searchParams.delete('expiresAt')
    url.searchParams.delete('answer')
    return url.origin === self.location.origin ? url.href : '/user'
  } catch { return '/user' }
}

self.addEventListener('install', (event) => event.waitUntil(self.skipWaiting()))
self.addEventListener('activate', (event) => {
  event.waitUntil(caches.keys()
    .then((keys) => Promise.all(keys.filter((key) => key.startsWith('onmaeum-shell-')).map((key) => caches.delete(key))))
    .then(() => self.clients.claim()))
})

self.addEventListener('push', (event) => {
  const data = parsePayload(event)
  const response = responseNotification(data)
  const canAnswer = response && Date.parse(response.expiresAt) > Date.now()
  event.waitUntil(Promise.all([
    self.registration.showNotification(data.title || 'On:마음 안심 알림', {
      body: data.body || '앱을 열어 알림을 확인해 주세요.',
      icon: '/android-chrome-192x192.png', badge: '/favicon-32x32.png',
      tag: data.notificationId ? `notification-${data.notificationId}` : undefined,
      requireInteraction: true,
      actions: canAnswer ? [
        { action: 'yes', title: '도움이 필요해요' },
        { action: 'no', title: '괜찮아요' },
      ] : [],
      data: { url: notificationUrl(data), ...response },
    }),
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
      clients.forEach((client) => client.postMessage({
        type: 'PUSH_RECEIVED', ...response, title: data.title, body: data.body,
      }))
    }),
  ]))
})

self.addEventListener('pushsubscriptionchange', (event) => {
  event.waitUntil(self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
    clients.forEach((client) => client.postMessage({ type: 'PUSH_SUBSCRIPTION_CHANGED' }))
  }))
})

self.addEventListener('notificationclick', (event) => {
  event.notification.close()
  event.waitUntil((async () => {
    const data = event.notification.data || {}
    const target = new URL(notificationUrl(data), self.location.origin)
    const response = responseNotification(data)
    if (response && Date.parse(response.expiresAt) > Date.now() &&
        (event.action === 'yes' || event.action === 'no')) {
      target.searchParams.set('answer', event.action)
    }
    const clients = await self.clients.matchAll({ type: 'window', includeUncontrolled: true })
    const client = clients.find((item) => {
      const pathname = new URL(item.url).pathname
      return pathname === '/user' || pathname.startsWith('/user/')
    })
    if (client && 'navigate' in client) {
      const navigated = await client.navigate(target.href)
      if (navigated) { await navigated.focus(); return }
    }
    await self.clients.openWindow(target.href)
  })())
})
