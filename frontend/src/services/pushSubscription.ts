import {
  deletePushSubscription,
  registerPushSubscription,
  type PushSubscriptionRequest,
} from "../api/monitoring";
import { getAppServiceWorkerRegistration } from "./serviceWorker";

export type PushSubscriptionStatus =
  | "unsupported"
  | "unconfigured"
  | "permission-required"
  | "denied"
  | "syncing"
  | "subscribed"
  | "error";

const vapidPublicKey = import.meta.env.VITE_WEB_PUSH_VAPID_PUBLIC_KEY?.trim() ?? "";
let synchronizationStopped = false;
let synchronizationQueue: Promise<void> = Promise.resolve();
let unsubscribePromise: Promise<void> | null = null;

function enqueueSynchronization<T>(operation: () => Promise<T>) {
  const result = synchronizationQueue.then(operation, operation);
  synchronizationQueue = result.then(
    () => undefined,
    () => undefined,
  );
  return result;
}

export function resumePushSubscriptionSynchronization() {
  if (!unsubscribePromise) synchronizationStopped = false;
}

export function getInitialPushStatus(): PushSubscriptionStatus {
  if (!("serviceWorker" in navigator) || !("PushManager" in window) || !("Notification" in window)) {
    return "unsupported";
  }
  if (!vapidPublicKey) return "unconfigured";
  if (Notification.permission === "denied") return "denied";
  return Notification.permission === "granted" ? "syncing" : "permission-required";
}

export function decodeVapidPublicKey(value: string) {
  const padding = "=".repeat((4 - (value.length % 4)) % 4);
  const base64 = (value + padding).replace(/-/g, "+").replace(/_/g, "/");
  const decoded = atob(base64);
  const bytes = new Uint8Array(decoded.length);
  for (let index = 0; index < decoded.length; index += 1) {
    bytes[index] = decoded.charCodeAt(index);
  }
  return bytes;
}

function usesVapidKey(subscription: PushSubscription, expected: Uint8Array<ArrayBuffer>) {
  const current = subscription.options.applicationServerKey;
  if (!current) return false;
  const currentBytes = new Uint8Array(current);
  return (
    currentBytes.length === expected.length &&
    currentBytes.every((value, index) => value === expected[index])
  );
}

function isExpired(subscription: PushSubscription) {
  return subscription.expirationTime !== null && subscription.expirationTime <= Date.now();
}

function toRequest(subscription: PushSubscription): PushSubscriptionRequest {
  const json = subscription.toJSON();
  const p256dh = json.keys?.p256dh;
  const auth = json.keys?.auth;
  if (!p256dh || !auth) throw new Error("알림 구독 정보를 확인할 수 없습니다.");

  return {
    endpoint: subscription.endpoint,
    expirationTime: subscription.expirationTime,
    keys: { p256dh, auth },
  };
}

async function getCurrentSubscription(applicationServerKey: Uint8Array<ArrayBuffer>) {
  const registration = await getAppServiceWorkerRegistration();
  let subscription = await registration.pushManager.getSubscription();

  if (subscription && (isExpired(subscription) || !usesVapidKey(subscription, applicationServerKey))) {
    await subscription.unsubscribe();
    subscription = null;
  }

  if (!subscription) {
    subscription = await registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey,
    });
  }

  return subscription;
}

async function performPushSubscriptionSynchronization(requestPermission: boolean) {
  const initialStatus = getInitialPushStatus();
  if (initialStatus === "unsupported" || initialStatus === "unconfigured") return initialStatus;

  let permission = Notification.permission;
  if (permission === "default" && requestPermission) {
    permission = await Notification.requestPermission();
  }
  if (permission === "default") return "permission-required";
  if (permission === "denied") return "denied";

  const applicationServerKey = decodeVapidPublicKey(vapidPublicKey);
  const subscription = await getCurrentSubscription(applicationServerKey);
  await registerPushSubscription(toRequest(subscription));
  return "subscribed";
}

export function synchronizePushSubscription(requestPermission: boolean) {
  if (synchronizationStopped) return Promise.resolve(getInitialPushStatus());

  return enqueueSynchronization(() => {
    if (synchronizationStopped) return Promise.resolve(getInitialPushStatus());
    return performPushSubscriptionSynchronization(requestPermission);
  });
}

async function performPushUnsubscribe() {
  await synchronizationQueue;
  if (!("serviceWorker" in navigator) || !("PushManager" in window)) return;
  const registration = await navigator.serviceWorker.getRegistration("/");
  const subscription = await registration?.pushManager.getSubscription();
  if (!subscription) return;

  try {
    await deletePushSubscription(subscription.endpoint);
  } finally {
    await subscription.unsubscribe();
  }
}

export function unsubscribeFromPush() {
  synchronizationStopped = true;
  if (!unsubscribePromise) {
    unsubscribePromise = performPushUnsubscribe().finally(() => {
      unsubscribePromise = null;
    });
  }
  return unsubscribePromise;
}
