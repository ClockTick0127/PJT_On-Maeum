import { useEffect, useRef } from "react";
import { Lottie } from "lottie-react";
import checkAnimation from "../../assets/lottie/check.json";
import type { MyDashboard } from "../../types/monitoring";
import Icon from "../common/Icon";

type UserStatusBarProps = {
  data?: MyDashboard;
  notice: UserStatusNotice;
  noticeVisible: boolean;
  notificationPending: boolean;
  notificationDisabled: boolean;
  onAnswerNotification: (answer: "yes" | "no") => void;
};

export type UserStatusNotice = {
  message: string;
  tone: "success" | "error" | "info";
} | null;

export default function UserStatusBar({
  data,
  notice,
  noticeVisible,
  notificationPending,
  notificationDisabled,
  onAnswerNotification,
}: UserStatusBarProps) {
  const isLoading = !data;
  const isAway = Boolean(data?.awayMode.enabled);
  const statusText = isLoading ? "상태 확인 중" : isAway ? "외출 중" : "재실 중";
  const statusKey = isLoading ? "loading" : isAway ? "away" : "home";
  const usesAnimatedCheck =
    notice?.tone === "success" &&
    (notice.message === "외출 설정 완료" ||
      notice.message === "귀가 설정 완료" ||
      notice.message === "도움 요청이 접수됐어요." ||
      notice.message === "괜찮다는 응답이 접수됐어요.");
  const showNotice = noticeVisible && !notificationPending;
  const statusRow = useRef<HTMLDivElement>(null);
  const previousStatus = useRef(statusKey);

  useEffect(() => {
    if (previousStatus.current === statusKey) return;
    previousStatus.current = statusKey;
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

    const animation = statusRow.current?.animate(
      [
        { opacity: 0, transform: "translateY(6px)" },
        { opacity: 1, transform: "translateY(0)" },
      ],
      { duration: 280, easing: "cubic-bezier(0.22, 1, 0.36, 1)" },
    );

    return () => animation?.cancel();
  }, [statusKey]);

  return (
    <>
      <div
        aria-hidden="true"
        className={`pointer-events-none fixed inset-0 z-40 bg-stone-950/20 transition-opacity duration-300 ease-out motion-reduce:transition-none ${notificationPending ? "opacity-100" : "opacity-0"}`}
      />
      <div
        className={`fixed inset-x-0 bottom-0 ${notificationPending ? "z-50" : "z-10"} mx-auto max-w-xl border-t px-6 pt-3.5 pb-[calc(.875rem+env(safe-area-inset-bottom))] shadow-[0_-8px_24px_rgb(41_37_36_/_7%)] transition-colors duration-300 ease-out motion-reduce:transition-none ${isAway ? "border-brand-200 bg-brand-50 text-brand-900" : "border-stone-200 bg-white text-stone-800"}`}
        aria-live="polite"
      >
        <div
          className={`grid transition-[grid-template-rows,margin,opacity] duration-300 ease-out motion-reduce:transition-none ${notificationPending ? "mb-3 grid-rows-[1fr] opacity-100" : "mb-0 grid-rows-[0fr] opacity-0"}`}
          aria-hidden={!notificationPending}
        >
          <div className="min-h-0 overflow-hidden">
            <section className="pb-3" aria-labelledby="response-title">
              <h2 className="text-[1.75rem] leading-snug font-bold" id="response-title">
                지금 상태를 알려주세요
              </h2>
              <div
                className={`grid transition-[grid-template-rows,margin,opacity] duration-200 ease-out motion-reduce:transition-none ${noticeVisible && notice?.tone === "error" ? "mt-2 grid-rows-[1fr] opacity-100" : "mt-0 grid-rows-[0fr] opacity-0"}`}
                aria-hidden={!(noticeVisible && notice?.tone === "error")}
              >
                <p
                  className="min-h-0 overflow-hidden text-base font-semibold text-red-700"
                  role={noticeVisible && notice?.tone === "error" ? "alert" : undefined}
                >
                  {notice?.message}
                </p>
              </div>
              <div className="mt-3 grid grid-cols-1 gap-3 min-[448px]:grid-cols-2">
                <button
                  className="flex min-h-14 items-center justify-center whitespace-nowrap rounded-full border border-[#9b312c] bg-[linear-gradient(180deg,#c9554e_0%,#b33b35_58%,#a3322d_100%)] px-2 py-2 text-2xl leading-tight font-semibold text-white shadow-[inset_0_1px_0_rgb(255_255_255/25%),0_5px_0_#842722,0_7px_14px_rgb(132_39_34/16%)] transition-[transform,box-shadow,filter] duration-150 hover:not-disabled:brightness-[1.02] active:not-disabled:translate-y-[3px] active:not-disabled:brightness-[.97] active:not-disabled:shadow-[inset_0_1px_0_rgb(255_255_255/18%),0_2px_0_#842722,0_3px_6px_rgb(132_39_34/10%)] disabled:cursor-not-allowed disabled:border-stone-200 disabled:bg-none disabled:bg-stone-200 disabled:text-stone-600 disabled:shadow-none"
                  type="button"
                  disabled={notificationDisabled || !notificationPending}
                  onClick={() => onAnswerNotification("yes")}
                >
                  도움이 필요해요
                </button>
                <button
                  className="flex min-h-14 items-center justify-center whitespace-nowrap rounded-full border border-[#b9c1c9] bg-[linear-gradient(180deg,#ffffff_0%,#f1f3f5_58%,#e3e7eb_100%)] px-2 py-2 text-2xl leading-tight font-semibold text-stone-800 shadow-[inset_0_1px_0_rgb(255_255_255/90%),0_5px_0_#b1bac3,0_7px_14px_rgb(15_23_42/10%)] transition-[transform,box-shadow,filter] duration-150 hover:not-disabled:brightness-[.99] active:not-disabled:translate-y-[3px] active:not-disabled:shadow-[inset_0_1px_0_rgb(255_255_255/70%),0_2px_0_#b1bac3,0_3px_6px_rgb(15_23_42/8%)] disabled:cursor-not-allowed disabled:border-stone-200 disabled:bg-none disabled:bg-stone-200 disabled:text-stone-600 disabled:shadow-none"
                  type="button"
                  disabled={notificationDisabled || !notificationPending}
                  onClick={() => onAnswerNotification("no")}
                >
                  괜찮아요
                </button>
              </div>
            </section>
          </div>
        </div>
        <div
          className={`grid transition-[grid-template-rows,margin,opacity] duration-300 ease-out motion-reduce:transition-none ${showNotice ? "mb-3 grid-rows-[1fr] opacity-100" : "mb-0 grid-rows-[0fr] opacity-0"}`}
          aria-hidden={!showNotice}
        >
          <div className="min-h-0 overflow-hidden">
            <div
              className={`flex items-center gap-2.5 pb-3 text-lg font-semibold ${notice?.tone === "error" ? "text-red-700" : "text-stone-800"}`}
              role={showNotice ? (notice?.tone === "error" ? "alert" : "status") : undefined}
            >
              {notice?.tone === "error" || notice?.tone === "info" ? (
                <Icon name="bell" />
              ) : usesAnimatedCheck ? (
                <Lottie
                  key={`${notice?.message}-${showNotice}`}
                  className="-my-1 size-8 shrink-0"
                  src={checkAnimation}
                  autoplay
                  loop={false}
                  aria-hidden="true"
                />
              ) : (
                <Icon name="check" />
              )}
              <span>{notice?.message}</span>
            </div>
          </div>
        </div>
        <div ref={statusRow} className="flex items-center justify-center py-1 text-center">
          <div className="min-w-0">
            <p className="text-[1.625rem] leading-tight font-bold tracking-[-0.02em]">
              {statusText}
            </p>
            {!isLoading && <p className="mt-1 text-lg leading-snug text-stone-600">{isAway ? "집 밖에 계신 상태예요" : "집에 계신 상태예요"}</p>}
          </div>
        </div>
      </div>
    </>
  );
}
