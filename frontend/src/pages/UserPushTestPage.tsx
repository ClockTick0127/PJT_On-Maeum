import { motion, useReducedMotion } from "motion/react";
import { useCallback, useEffect, useRef, useState } from "react";
import type { CSSProperties } from "react";
import { useNavigate } from "react-router-dom";
import { getApiErrorMessage } from "../api/client";
import UserAwayTab from "../components/user/UserAwayTab";
import UserHeader from "../components/user/UserHeader";
import UserHomeTab from "../components/user/UserHomeTab";
import UserStatusBar from "../components/user/UserStatusBar";
import type { UserStatusNotice } from "../components/user/UserStatusBar";
import type { MyDashboard } from "../types/monitoring";
import { useNotificationAction } from "../hooks/notifications/useNotificationAction";
import { usePendingNotification } from "../hooks/notifications/usePendingNotification";
import { waitForActiveServiceWorker } from "../services/serviceWorker";

const initialDashboard: MyDashboard = {
  subjectId: "push-test-user",
  name: "박정수",
  awayMode: { enabled: false, scheduled: false, startedAt: null, until: null },
  manager: { name: "이돌봄", phone: "02-0000-0000" },
};

async function getTestRegistration() {
  if (!window.isSecureContext || !("serviceWorker" in navigator) || !("Notification" in window)) {
    throw new Error("브라우저 알림은 HTTPS 또는 localhost의 지원 브라우저에서 테스트해 주세요.");
  }
  const permission = Notification.permission === "default"
    ? await Notification.requestPermission() : Notification.permission;
  if (permission !== "granted") throw new Error("브라우저 설정에서 알림을 허용해 주세요.");
  const registration = await navigator.serviceWorker.register("/user-push-test-sw.js", {
    scope: "/user-push-test",
  });
  return waitForActiveServiceWorker(registration);
}

export default function UserPushTestPage() {
  const reduceMotion = useReducedMotion();
  const navigate = useNavigate();
  const { notificationId, canAnswer, clearNotification, receiveNotification: receiveResponseNotification, information } = usePendingNotification("TEST_PUSH_RECEIVED");
  const [data, setData] = useState(initialDashboard);
  const [tab, setTab] = useState<"home" | "away">("home");
  const [draft, setDraft] = useState({ duration: 60 });
  const pending = Boolean(notificationId);
  const [notice, setNotice] = useState<UserStatusNotice>(null);
  const [noticeVisible, setNoticeVisible] = useState(false);
  const [busy, setBusy] = useState(false);
  const [sending, setSending] = useState(false);
  const [failResponse, setFailResponse] = useState(false);
  const [toolsVisible, setToolsVisible] = useState(true);
  const [awayButtonHeight, setAwayButtonHeight] = useState<number | null>(null);
  const [originalAwayButtonHeight, setOriginalAwayButtonHeight] = useState<number | null>(null);
  const page = useRef<HTMLElement>(null);
  const [result, setResult] = useState("테스트 대기 중");
  const noticeTimer = useRef<number | undefined>(undefined);
  const responseTimer = useRef<number | undefined>(undefined);
  const receiveTimer = useRef<number | undefined>(undefined);

  useEffect(() => {
    const button = page.current?.querySelector<HTMLButtonElement>('[data-user-action="away"]');
    if (!button) return;
    const observer = new ResizeObserver(() => {
      setOriginalAwayButtonHeight((height) => height ?? button.getBoundingClientRect().height);
    });
    observer.observe(button);
    return () => observer.disconnect();
  }, [tab, data.awayMode.enabled]);

  useEffect(() => {
    const receive = (event: MessageEvent) => {
      if (event.data?.type !== "TEST_PUSH_RECEIVED") return;
      setResult("서비스 워커의 push 이벤트 수신");
    };
    navigator.serviceWorker?.addEventListener("message", receive);
    return () => {
      navigator.serviceWorker?.removeEventListener("message", receive);
      window.clearTimeout(noticeTimer.current);
      window.clearTimeout(responseTimer.current);
      window.clearTimeout(receiveTimer.current);
    };
  }, []);

  const showNotice = useCallback((message: string, tone: "success" | "error" | "info" = "success") => {
    window.clearTimeout(noticeTimer.current);
    setNotice({ message, tone });
    setNoticeVisible(true);
    noticeTimer.current = window.setTimeout(() => setNoticeVisible(false), tone === "info" ? 6000 : 3000);
  }, []);

  useEffect(() => {
    if (!information) return;
    const timer = window.setTimeout(() => showNotice(information.message, "info"), 0);
    return () => window.clearTimeout(timer);
  }, [information, showNotice]);

  const receiveNotification = () => {
    setNoticeVisible(false);
    receiveResponseNotification({ notificationId: String(Date.now()), expiresAt: new Date(Date.now() + 30_000).toISOString() });
    setTab("home");
    setResult("위험 알림 수신 재현 · 응답 대기");
  };

  const sendBrowserNotification = async () => {
    setSending(true);
    try {
      const registration = await getTestRegistration();
      const response = { notificationId: String(Date.now()), expiresAt: new Date(Date.now() + 30_000).toISOString() };
      const options: NotificationOptions & {
        actions: { action: string; title: string }[];
      } = {
        body: "생활 신호가 확인되지 않았어요. 지금 상태를 알려주세요.",
        icon: "/android-chrome-192x192.png",
        badge: "/favicon-32x32.png",
        tag: `onmaeum-user-push-test-${response.notificationId}`,
        requireInteraction: true,
        actions: [
          { action: "yes", title: "도움이 필요해요" },
          { action: "no", title: "괜찮아요" },
        ],
        data: { ...response, url: `/user-push-test?${new URLSearchParams(response)}` },
      };
      await registration.showNotification("On:마음 안심 알림 · 테스트", options);
      receiveResponseNotification(response);
      setTab("home");
      setResult("브라우저에 테스트 알림 표시를 요청했어요. 보이지 않으면 브라우저·기기 알림 설정을 확인해 주세요.");
    } catch (error) {
      setResult(getApiErrorMessage(error));
    } finally {
      setSending(false);
    }
  };

  const answer = (value: "yes" | "no") => {
    if (busy || !notificationId || !canAnswer()) return;
    setBusy(true);
    setResult("응답 처리 중 · 테스트 데이터");
    responseTimer.current = window.setTimeout(() => {
      setBusy(false);
      if (failResponse) {
        showNotice("응답을 전달하지 못했어요. 다시 선택해 주세요.", "error");
        setResult("응답 실패 재현 · 요청 유지");
        return;
      }
      clearNotification(notificationId);
      showNotice(value === "yes" ? "도움 요청이 접수됐어요." : "괜찮다는 응답이 접수됐어요.");
      setResult(
        value === "yes"
          ? "도움 필요 응답 완료 · 서버 전송 없음"
          : "괜찮아요 응답 완료 · 서버 전송 없음",
      );
    }, 700);
  };

  useNotificationAction(answer, pending && !busy);

  const updateAway = (enabled: boolean, duration?: number) => {
    if (busy) return;
    if (enabled && duration !== undefined && (!Number.isInteger(duration) || duration < 1 || duration > 1440)) {
      showNotice("외출 시간을 1~1440분 사이로 입력해 주세요.", "error");
      return;
    }
    setData((current) => ({
      ...current,
      awayMode: {
        enabled,
        scheduled: false,
        startedAt: enabled ? new Date().toISOString() : null,
        until: enabled && duration !== undefined ? new Date(Date.now() + duration * 60_000).toISOString() : null,
      },
    }));
    setTab("home");
    showNotice(enabled ? "외출 설정 완료" : "귀가 설정 완료");
  };

  const status = {
    dashboardError: "",
    pushStatus: "subscribed" as const,
    pushError: "",
    onRetryDashboard: () => {},
    onEnablePush: () => {},
    onRetryPush: () => {},
  };
  const testButton =
    "min-h-12 rounded-xl border border-stone-300 bg-white px-4 py-3 text-base font-semibold hover:bg-stone-50 disabled:opacity-50";

  return (
    <main
      ref={page}
      style={
        originalAwayButtonHeight === null
          ? undefined
          : ({
              "--test-away-height": `${awayButtonHeight ?? originalAwayButtonHeight}px`,
            } as CSSProperties)
      }
      className={`min-h-screen touch-manipulation bg-stone-50 text-xl leading-[1.6] text-stone-800 transition-[padding] duration-300 [overflow-wrap:anywhere] [&_[data-user-action=away]]:transition-[height,transform,box-shadow,filter] [&_[data-user-action=away]]:duration-300 [&_[data-user-action=away]]:ease-out ${originalAwayButtonHeight !== null ? "[&_[data-user-action=away]]:h-[var(--test-away-height)]" : ""} ${pending ? "pb-[calc(15rem+env(safe-area-inset-bottom))]" : noticeVisible ? "pb-[calc(11rem+env(safe-area-inset-bottom))]" : "pb-[calc(7rem+env(safe-area-inset-bottom))]"}`}
    >
      <UserHeader
        onGoHome={() => navigate("/")}
        onOpenTestTools={() => setToolsVisible(true)}
        onLogout={() => setResult("테스트 화면에서는 로그아웃하지 않습니다.")}
      />
      <motion.div
        key={tab}
        initial={reduceMotion ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.18 }}
        className="mx-auto grid max-w-xl gap-5 px-5"
      >
        {toolsVisible && (
          <section
            className="rounded-2xl border border-stone-200 bg-white p-5 text-base"
            aria-label="위험 알림 테스트 도구"
          >
            <div className="flex items-center justify-between gap-3">
              <h1 className="text-lg font-bold">대상자 위험 알림 테스트</h1>
              <button
                className="min-h-11 px-2 text-sm font-semibold text-stone-600 underline"
                onClick={() => setToolsVisible(false)}
              >
                캡처 모드
              </button>
            </div>
            <p className="mt-2 text-sm text-stone-600">
              로그인 없이 테스트 데이터로 실제 대상자 UI를 확인합니다. 응답은 서버에 저장되지
              않습니다. 브라우저 알림은 로컬 재현이며 서버 Web Push 발송 테스트는 아닙니다.
              테스트 응답 기한은 30초이며, 운영 화면은 서버가 보낸 실제 기한을 사용합니다.
            </p>
            <div className="mt-4 grid grid-cols-2 gap-3">
              <button
                className={testButton}
                disabled={busy || sending}
                onClick={() => receiveNotification()}
              >
                화면 알림 재현
              </button>
              <button
                className={testButton}
                disabled={busy || sending}
                onClick={() => void sendBrowserNotification()}
              >
                {sending ? "알림 준비 중…" : "브라우저 알림 표시"}
              </button>
              <button
                className={testButton}
                disabled={busy || sending}
                onClick={() => {
                  window.clearTimeout(receiveTimer.current);
                  setResult("5초 뒤 화면 알림 표시");
                  receiveTimer.current = window.setTimeout(receiveNotification, 5000);
                }}
              >
                5초 뒤 화면 알림
              </button>
              <button
                className={testButton}
                disabled={busy || sending}
                onClick={() => {
                  window.clearTimeout(receiveTimer.current);
                  window.clearTimeout(noticeTimer.current);
                  clearNotification(notificationId ?? undefined);
                  setNoticeVisible(false);
                  setResult("알림 초기화");
                }}
              >
                알림 초기화
              </button>
              <button
                className={testButton}
                disabled={busy || sending}
                onClick={() => receiveResponseNotification({ notificationId: String(Date.now()), expiresAt: new Date(Date.now() + 5000).toISOString() })}
              >
                5초 뒤 응답 기한 만료
              </button>
              <button
                className={testButton}
                disabled={busy || sending || pending}
                onClick={() => receiveResponseNotification({ title: "생활 안내", body: "응답이 필요하지 않은 안내 알림이에요." })}
              >
                응답 없는 안내 알림
              </button>
            </div>
            <button
              className={`${testButton} mt-3 w-full`}
              aria-pressed={awayButtonHeight !== null}
              disabled={awayButtonHeight === null && (tab !== "home" || data.awayMode.enabled)}
              onClick={() => {
                if (awayButtonHeight !== null) {
                  setAwayButtonHeight(null);
                  return;
                }
                if (originalAwayButtonHeight !== null)
                  setAwayButtonHeight(originalAwayButtonHeight * 2);
              }}
            >
              {awayButtonHeight !== null ? "외출하기 원래 높이로 복원" : "외출하기 높이 2배"}
            </button>
            <label className="mt-4 flex min-h-11 items-center gap-3">
              <input
                className="size-5 accent-brand-500"
                type="checkbox"
                checked={failResponse}
                disabled={busy}
                onChange={(event) => setFailResponse(event.target.checked)}
              />
              응답 실패 재현
            </label>
            <p className="mt-2 rounded-xl bg-stone-100 p-3 text-sm" role="status">
              {result}
            </p>
            <details className="mt-3 text-sm text-stone-600">
              <summary className="min-h-11 cursor-pointer py-2 font-semibold">
                서비스 워커 push 이벤트 확인 방법
              </summary>
              <p className="mt-2">
                브라우저 알림 표시를 한 번 누른 뒤 Chrome 개발자 도구 → Application → Service
                Workers에서 user-push-test-sw.js의 Push를 누르세요. 알림 클릭 시 이 테스트 페이지로
                돌아옵니다. HTTPS 또는 localhost와 알림 허용이 필요합니다.
              </p>
              <p className="mt-2">
                알림 모양은 운영체제가 결정합니다. 하단 응답 영역과 완료 애니메이션은 서비스
                컴포넌트를 그대로 사용합니다.
              </p>
            </details>
          </section>
        )}
        {tab === "home" ? (
          <UserHomeTab
            data={data}
            comparison={false}
            busy={busy}
            status={status}
            onUpdateAway={updateAway}
          />
        ) : (
          <UserAwayTab
            draft={draft}
            busy={busy}
            dataAvailable
            comparison={false}
            status={status}
            onDraftChange={setDraft}
            onStartAway={(duration) => updateAway(true, duration)}
            onBack={() => setTab("home")}
          />
        )}
      </motion.div>
      <UserStatusBar
        data={data}
        notice={notice}
        noticeVisible={noticeVisible}
        notificationPending={pending}
        notificationDisabled={busy}
        onAnswerNotification={answer}
      />
    </main>
  );
}
