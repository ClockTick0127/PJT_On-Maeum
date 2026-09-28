import { useCallback, useEffect, useState } from "react";
import { getApiErrorMessage } from "../../api/client";
import {
  getInitialPushStatus,
  resumePushSubscriptionSynchronization,
  synchronizePushSubscription,
  unsubscribeFromPush,
  type PushSubscriptionStatus,
} from "../../services/pushSubscription";

export function usePushSubscription() {
  const [status, setStatus] = useState<PushSubscriptionStatus>(getInitialPushStatus);
  const [error, setError] = useState("");

  const synchronize = useCallback(async (requestPermission: boolean) => {
    setStatus("syncing");
    setError("");
    try {
      setStatus(await synchronizePushSubscription(requestPermission));
    } catch (cause) {
      setStatus("error");
      setError(getApiErrorMessage(cause));
    }
  }, []);

  useEffect(() => {
    resumePushSubscriptionSynchronization();

    const refresh = () => {
      if (document.visibilityState === "visible") void synchronize(false);
    };
    const receive = (event: MessageEvent) => {
      if (event.data?.type === "PUSH_SUBSCRIPTION_CHANGED") void synchronize(false);
    };

    const initialization = window.setTimeout(() => void synchronize(false), 0);
    window.addEventListener("online", refresh);
    document.addEventListener("visibilitychange", refresh);
    navigator.serviceWorker?.addEventListener("controllerchange", refresh);
    navigator.serviceWorker?.addEventListener("message", receive);

    return () => {
      window.clearTimeout(initialization);
      window.removeEventListener("online", refresh);
      document.removeEventListener("visibilitychange", refresh);
      navigator.serviceWorker?.removeEventListener("controllerchange", refresh);
      navigator.serviceWorker?.removeEventListener("message", receive);
    };
  }, [synchronize]);

  return {
    status,
    error,
    enable: () => synchronize(true),
    retry: () => synchronize(false),
    unsubscribe: unsubscribeFromPush,
  };
}
