import { useCallback, useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { isResponseNotificationActive, readResponseNotification } from "../../utils/userNotifications";
import type { ResponseNotification } from "../../utils/userNotifications";

type PushMessage = { notificationId?: unknown; expiresAt?: unknown; title?: unknown; body?: unknown };

export function usePendingNotification(messageType = "PUSH_RECEIVED") {
  const [searchParams, setSearchParams] = useSearchParams();
  const urlId = searchParams.get("notificationId");
  const urlExpiry = searchParams.get("expiresAt");
  const [notification, setNotification] = useState<ResponseNotification | null>(() =>
    readResponseNotification({ notificationId: urlId, expiresAt: urlExpiry }),
  );
  const [information, setInformation] = useState<{ message: string; receivedAt: number } | null>(null);
  const closedIds = useRef(new Set<string>());

  const clearNotification = useCallback((expectedId?: string) => {
    if (expectedId) closedIds.current.add(expectedId);
    setNotification((current) => !expectedId || current?.notificationId === expectedId ? null : current);
    const params = new URLSearchParams(window.location.search);
    if (expectedId && params.get("notificationId") !== expectedId) return;
    params.delete("notificationId");
    params.delete("expiresAt");
    params.delete("answer");
    setSearchParams(params, { replace: true });
  }, [setSearchParams]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      if (!urlId) return;
      const next = readResponseNotification({ notificationId: urlId, expiresAt: urlExpiry });
      if (!isResponseNotificationActive(next) || closedIds.current.has(urlId)) {
        clearNotification(urlId);
        return;
      }
      setNotification(next);
    }, 0);
    return () => window.clearTimeout(timer);
  }, [urlId, urlExpiry, clearNotification]);

  const receiveNotification = useCallback((payload: PushMessage) => {
    const next = readResponseNotification(payload);
    if (!next) {
      const message = typeof payload.body === "string" && payload.body.trim()
        ? payload.body : typeof payload.title === "string" ? payload.title : "새로운 알림이 도착했어요.";
      setInformation({ message, receivedAt: Date.now() });
      return;
    }
    if (closedIds.current.has(next.notificationId)) return;
    if (!isResponseNotificationActive(next)) {
      setInformation({ message: "안부 확인 응답 시간이 지났어요.", receivedAt: Date.now() });
      return;
    }
    setNotification(next);
    const params = new URLSearchParams(window.location.search);
    params.set("notificationId", next.notificationId);
    params.set("expiresAt", next.expiresAt);
    params.delete("answer");
    setSearchParams(params, { replace: true });
  }, [setSearchParams]);

  useEffect(() => {
    const receive = (event: MessageEvent) => {
      if (event.data?.type === messageType) receiveNotification(event.data);
    };

    navigator.serviceWorker?.addEventListener("message", receive);
    return () => navigator.serviceWorker?.removeEventListener("message", receive);
  }, [messageType, receiveNotification]);

  useEffect(() => {
    if (!notification) return;
    let timer: number;
    const checkExpiry = () => {
      window.clearTimeout(timer);
      const remaining = Date.parse(notification.expiresAt) - Date.now();
      if (remaining <= 0) {
        clearNotification(notification.notificationId);
        setInformation({ message: "안부 확인 응답 시간이 지났어요.", receivedAt: Date.now() });
        return;
      }
      timer = window.setTimeout(checkExpiry, Math.min(remaining, 2_147_483_647));
    };
    checkExpiry();
    window.addEventListener("focus", checkExpiry);
    document.addEventListener("visibilitychange", checkExpiry);
    return () => {
      window.clearTimeout(timer);
      window.removeEventListener("focus", checkExpiry);
      document.removeEventListener("visibilitychange", checkExpiry);
    };
  }, [notification, clearNotification]);

  return {
    notificationId: isResponseNotificationActive(notification) ? notification?.notificationId ?? null : null,
    canAnswer: () => isResponseNotificationActive(notification),
    clearNotification,
    receiveNotification,
    information,
  };
}
