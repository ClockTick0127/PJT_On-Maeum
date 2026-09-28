import { motion, useReducedMotion } from "motion/react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { getApiErrorMessage } from "../api/client";
import { clearSession } from "../api/tokenStorage";
import type { AwayDraft } from "../components/user/AwaySettings";
import UserAwayTab from "../components/user/UserAwayTab";
import UserHeader from "../components/user/UserHeader";
import UserHomeTab from "../components/user/UserHomeTab";
import UserStatusBar from "../components/user/UserStatusBar";
import type { UserStatusNotice } from "../components/user/UserStatusBar";
import type { UserTabStatusProps } from "../components/user/UserTabStatus";
import {
  useAwayModeMutation,
  useMyDashboardQuery,
  useNotificationResponseMutation,
} from "../hooks/api";
import { usePendingNotification } from "../hooks/notifications/usePendingNotification";
import { useNotificationAction } from "../hooks/notifications/useNotificationAction";
import { usePushSubscription } from "../hooks/notifications/usePushSubscription";
import { isUnavailableNotificationError } from "../utils/userNotifications";

type UserView = "home" | "away";

export default function UserPage({ comparison = false }: { comparison?: boolean }) {
  const reduceMotion = useReducedMotion();
  const navigate = useNavigate();
  const dashboard = useMyDashboardQuery();
  const awayMutation = useAwayModeMutation();
  const responseMutation = useNotificationResponseMutation();
  const [tab, setTab] = useState<UserView>("home");
  const [awayDraft, setAwayDraft] = useState<AwayDraft>({ duration: 60 });
  const [statusNotice, setStatusNotice] = useState<UserStatusNotice>(null);
  const [statusNoticeVisible, setStatusNoticeVisible] = useState(false);
  const statusNoticeTimer = useRef<number | undefined>(undefined);
  const statusNoticeFrame = useRef<number | undefined>(undefined);
  const { notificationId, clearNotification, canAnswer, information } = usePendingNotification();
  const pushSubscription = usePushSubscription();
  const data = dashboard.data;
  const busy = awayMutation.isPending || responseMutation.isPending;
  const notificationPending = Boolean(notificationId);

  useEffect(
    () => () => {
      window.clearTimeout(statusNoticeTimer.current);
      window.cancelAnimationFrame(statusNoticeFrame.current ?? 0);
    },
    [],
  );

  const showStatusNotice = useCallback((message: string, tone: "success" | "error" | "info" = "success") => {
    window.clearTimeout(statusNoticeTimer.current);
    window.cancelAnimationFrame(statusNoticeFrame.current ?? 0);
    setStatusNoticeVisible(false);
    setStatusNotice({ message, tone });
    statusNoticeFrame.current = window.requestAnimationFrame(() => {
      statusNoticeFrame.current = window.requestAnimationFrame(() => {
        setStatusNoticeVisible(true);
        statusNoticeTimer.current = window.setTimeout(
          () => setStatusNoticeVisible(false),
          tone === "info" ? 6000 : 3000,
        );
      });
    });
  }, []);

  useEffect(() => {
    if (!information) return;
    const timer = window.setTimeout(() => showStatusNotice(information.message, "info"), 0);
    return () => window.clearTimeout(timer);
  }, [information, showStatusNotice]);

  const updateAway = async (enabled: boolean, duration?: number) => {
    if (busy || !data) return;
    if (
      enabled &&
      duration !== undefined &&
      (!Number.isInteger(duration) || duration < 1 || duration > 1440)
    ) {
      showStatusNotice("외출 시간을 1~1440분 사이로 입력해 주세요.", "error");
      return;
    }
    try {
      await awayMutation.mutateAsync(
        enabled && duration !== undefined
          ? {
              enabled,
              endsAt: new Date(Date.now() + duration * 60_000).toISOString(),
            }
          : { enabled },
      );
      showStatusNotice(enabled ? "외출 설정 완료" : "귀가 설정 완료");
      if (enabled) setTab("home");
    } catch (cause) {
      showStatusNotice(getApiErrorMessage(cause), "error");
    }
  };

  const answer = async (value: "yes" | "no") => {
    if (!notificationId || busy) return;
    if (!canAnswer()) {
      clearNotification(notificationId);
      return;
    }
    try {
      await responseMutation.mutateAsync({ notificationId, answer: value });
      showStatusNotice(
        value === "yes" ? "도움 요청이 접수됐어요." : "괜찮다는 응답이 접수됐어요.",
      );
      clearNotification(notificationId);
    } catch (cause) {
      if (isUnavailableNotificationError(cause)) clearNotification(notificationId);
      showStatusNotice(getApiErrorMessage(cause), "error");
    }
  };

  useNotificationAction(answer, Boolean(data) && notificationPending && !busy);

  const logout = async () => {
    await Promise.allSettled([pushSubscription.unsubscribe()]);
    clearSession();
    navigate("/");
  };

  const tabStatus: UserTabStatusProps = {
    dashboardError: dashboard.isError ? getApiErrorMessage(dashboard.error) : "",
    pushStatus: pushSubscription.status,
    pushError: pushSubscription.error,
    onRetryDashboard: () => void dashboard.refetch(),
    onEnablePush: () => void pushSubscription.enable(),
    onRetryPush: () => void pushSubscription.retry(),
  };

  return (
    <main
      className={`min-h-screen touch-manipulation bg-stone-50 text-xl leading-[1.6] text-stone-800 transition-[padding] duration-300 [overflow-wrap:anywhere] ${notificationPending ? "pb-[calc(15rem+env(safe-area-inset-bottom))]" : statusNoticeVisible ? "pb-[calc(11rem+env(safe-area-inset-bottom))]" : "pb-[calc(7rem+env(safe-area-inset-bottom))]"} ${comparison ? "selection:bg-brand-200" : ""}`}
    >
      <UserHeader onGoHome={() => navigate("/")} onLogout={logout} />
      <motion.div
        key={tab}
        initial={reduceMotion ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.18 }}
        className="mx-auto grid max-w-xl gap-5 px-5"
      >
        {tab === "home" ? (
          <UserHomeTab
            data={data}
            comparison={comparison}
            busy={busy}
            status={tabStatus}
            onUpdateAway={(enabled) => void updateAway(enabled)}
          />
        ) : (
          <UserAwayTab
            draft={awayDraft}
            busy={busy}
            dataAvailable={Boolean(data)}
            comparison={comparison}
            status={tabStatus}
            onDraftChange={setAwayDraft}
            onStartAway={(duration) => void updateAway(true, duration)}
            onBack={() => setTab("home")}
          />
        )}
      </motion.div>
      <UserStatusBar
        data={data}
        notice={statusNotice}
        noticeVisible={statusNoticeVisible}
        notificationPending={notificationPending}
        notificationDisabled={busy || !data}
        onAnswerNotification={(value) => void answer(value)}
      />
    </main>
  );
}
