import axios from "axios";

export type ResponseNotification = { notificationId: string; expiresAt: string };

export function readResponseNotification(payload: { notificationId?: unknown; expiresAt?: unknown }): ResponseNotification | null {
  const notificationId = String(payload.notificationId ?? "");
  if (!/^\d+$/.test(notificationId) || typeof payload.expiresAt !== "string" || !Number.isFinite(Date.parse(payload.expiresAt))) return null;
  return { notificationId, expiresAt: payload.expiresAt };
}

export function isResponseNotificationActive(notification: ResponseNotification | null, now = Date.now()) {
  return Boolean(notification && Date.parse(notification.expiresAt) > now);
}

export function isUnavailableNotificationError(error: unknown) {
  return axios.isAxiosError(error) && (error.response?.status === 404 || error.response?.status === 409);
}
