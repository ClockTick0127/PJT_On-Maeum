import { request } from "../client";

export type PushSubscriptionRequest = {
  endpoint: string;
  expirationTime: number | null;
  keys: {
    p256dh: string;
    auth: string;
  };
};

export const registerPushSubscription = (subscription: PushSubscriptionRequest) =>
  request<void>({
    url: "/monitoring/push-subscriptions",
    method: "POST",
    data: subscription,
  });

export const deletePushSubscription = (endpoint: string) =>
  request<void>({
    url: "/monitoring/push-subscriptions",
    method: "DELETE",
    data: { endpoint },
  });
