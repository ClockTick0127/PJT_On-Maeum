import { useEffect, useRef } from "react";
import { useAnimate, useReducedMotion } from "motion/react";
import type { Subject } from "../../types/monitoring";
import type { StaffNotice } from "../../utils/staffNotices";
import Icon from "../common/Icon";
import StreamNotice from "./StreamNotice";

type StaffNotificationsProps = {
  notices: StaffNotice[];
  subjects: Subject[];
  onDismiss: (id: string) => void;
  basePath?: string;
};

export default function StaffNotifications({ notices, subjects, onDismiss, basePath }: StaffNotificationsProps) {
  const [edge, animate] = useAnimate();
  const reduceMotion = useReducedMotion();
  const announced = useRef(new Set<string>());
  const list = useRef<HTMLElement>(null);
  const newest = notices[0];
  const signal = newest ? `${newest.id}:${newest.event.version}:${newest.event.updatedAt}` : "";
  const urgent = newest && newest.kind !== "safe";
  const helpCount = notices.filter((notice) => notice.kind === "help").length;
  const riskCount = notices.filter((notice) => notice.kind === "risk").length;
  const otherCount = notices.length - helpCount - riskCount;
  const summary = [helpCount && `도움 요청 ${helpCount}건`, riskCount && `위험 감지 ${riskCount}건`, otherCount && `응답 알림 ${otherCount}건`].filter(Boolean).join(" · ");

  useEffect(() => {
    if (!signal || announced.current.has(signal)) return;
    const element = edge.current;
    if (reduceMotion || !urgent || !element) {
      announced.current.add(signal);
      return;
    }
    let stopAnimation: (() => void) | undefined;
    const frame = requestAnimationFrame(() => {
      announced.current.add(signal);
      const animation = animate(element, { opacity: [0, 0.65, 0.15, 0.65, 0] }, { duration: 2.4, ease: "easeInOut" });
      stopAnimation = () => animation.stop();
    });
    return () => {
      cancelAnimationFrame(frame);
      stopAnimation?.();
      element.style.opacity = "0";
    };
  }, [signal, urgent, reduceMotion, animate, edge]);

  if (!notices.length) return null;

  return (
    <>
      <div
        ref={edge}
        aria-hidden="true"
        className={`pointer-events-none fixed inset-0 z-40 border-[3px] opacity-0 ${helpCount ? "border-red-500 shadow-[inset_0_0_32px_rgb(220_38_38_/_16%)]" : "border-orange-400 shadow-[inset_0_0_32px_rgb(243_121_41_/_14%)]"}`}
      />
      <button
        type="button"
        className={`fixed right-4 bottom-5 z-30 flex max-w-[calc(100%-2rem)] items-center gap-3 rounded-xl border px-5 py-4 text-left shadow-lg transition-colors md:right-8 md:bottom-8 ${helpCount ? "border-red-700 bg-red-700 text-white hover:bg-red-800" : riskCount ? "border-stone-800 bg-stone-800 text-white hover:bg-stone-900" : "border-stone-300 bg-white text-stone-800 hover:bg-stone-50"}`}
        onClick={() => {
          list.current?.scrollIntoView({ behavior: reduceMotion ? "instant" : "smooth", block: "start" });
          list.current?.focus({ preventScroll: true });
        }}
        aria-label={`${summary}. 알림 목록으로 이동`}
      >
        <Icon name={helpCount ? "phone" : "bell"} />
        <span>
          <span className="block text-sm opacity-80">새 알림</span>
          <span className="block font-semibold">{summary}</span>
        </span>
        <Icon name="arrow" />
      </button>
      <section ref={list} tabIndex={-1} aria-label="담당 대상자 알림" className="scroll-mt-6 space-y-3 rounded-xl focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-brand-500">
        {notices.map((notice) => (
          <StreamNotice
            key={notice.id}
            notice={notice}
            subjectName={subjects.find((subject) => subject.subjectId === notice.event.subjectId)?.name ?? "대상자"}
            onClose={() => onDismiss(notice.id)}
            basePath={basePath}
          />
        ))}
      </section>
    </>
  );
}
