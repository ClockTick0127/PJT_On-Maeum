import { useEffect, useRef } from "react";
import { useLocation } from "react-router-dom";

export function useNotificationAction(
  onAnswer: (answer: "yes" | "no") => void | Promise<void>,
  ready: boolean,
) {
  const { search } = useLocation();
  const handled = useRef("");

  useEffect(() => {
    const params = new URLSearchParams(search);
    const answer = params.get("answer");
    const notificationId = params.get("notificationId");
    if (
      !ready ||
      !notificationId ||
      !/^\d+$/.test(notificationId) ||
      (answer !== "yes" && answer !== "no")
    ) return;
    const key = `${notificationId}:${answer}`;
    if (handled.current === key) return;

    const timer = window.setTimeout(() => {
      handled.current = key;
      const url = new URL(window.location.href);
      url.searchParams.delete("answer");
      window.history.replaceState(window.history.state, "", url);
      void onAnswer(answer);
    }, 0);

    return () => window.clearTimeout(timer);
  }, [onAnswer, ready, search]);
}
