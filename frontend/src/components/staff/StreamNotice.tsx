import { motion, useReducedMotion } from "motion/react";
import { Link } from "react-router-dom";
import type { StaffNotice, StaffNoticeKind } from "../../utils/staffNotices";
import { formatDateTime } from "../../utils/format";
import Icon from "../common/Icon";
import type { IconName } from "../common/Icon";

const appearance: Record<StaffNoticeKind, {
  label: string;
  message: string;
  description: string;
  icon: IconName;
  card: string;
  text: string;
  action: string;
}> = {
  risk: {
    label: "위험 감지",
    message: "위험 신호가 감지되었습니다.",
    description: "대상자에게 연락하여 안부를 확인해 주세요.",
    icon: "bell",
    card: "border-stone-200 bg-white",
    text: "text-orange-800",
    action: "bg-stone-800 text-white hover:bg-stone-900",
  },
  help: {
    label: "도움 요청",
    message: "도움을 요청했습니다.",
    description: "‘도움이 필요해요’ 응답을 보냈습니다. 바로 연락해 주세요.",
    icon: "phone",
    card: "border-red-200 bg-red-50/60",
    text: "text-red-800",
    action: "bg-red-600 text-white hover:bg-red-700",
  },
  safe: {
    label: "안전 응답",
    message: "괜찮다고 응답했습니다.",
    description: "‘괜찮아요’ 응답이 도착했습니다.",
    icon: "check",
    card: "border-stone-200 bg-white",
    text: "text-emerald-800",
    action: "bg-stone-800 text-white hover:bg-stone-900",
  },
  expired: {
    label: "응답 시간 만료",
    message: "안부 확인 응답 시간이 지났습니다.",
    description: "아직 응답이 없습니다. 연락하여 상태를 확인해 주세요.",
    icon: "clock",
    card: "border-amber-200 bg-amber-50/60",
    text: "text-amber-800",
    action: "bg-stone-800 text-white hover:bg-stone-900",
  },
};

type StreamNoticeProps = {
  notice: StaffNotice;
  subjectName: string;
  onClose: () => void;
  basePath?: string;
};

export default function StreamNotice({ notice, subjectName, onClose, basePath = "/staff" }: StreamNoticeProps) {
  const reduceMotion = useReducedMotion();
  const style = appearance[notice.kind];
  const { event } = notice;

  return (
    <motion.aside
      initial={reduceMotion ? false : { opacity: 0, y: -6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.22 }}
      className={`flex flex-wrap items-center gap-x-4 gap-y-2 rounded-xl border px-4 py-3 ${style.card}`}
      role={notice.kind === "safe" ? "status" : "alert"}
    >
      <Icon className={style.text} name={style.icon} />
      <div className="min-w-0 flex-1 basis-56">
        <strong className="text-base leading-6 text-stone-900">{subjectName}님 · <span className={style.text}>{style.message}</span></strong>
        <p className="mt-0.5 text-sm leading-5 text-stone-600">{style.description}</p>
      </div>
      <span className={`whitespace-nowrap font-semibold tabular-nums ${style.text}`}>{event.riskScore}점</span>
      <time className="hidden whitespace-nowrap text-sm text-stone-500 md:block" dateTime={event.updatedAt}>{formatDateTime(event.updatedAt)}</time>
      <Link
        className={`inline-flex min-h-10 items-center whitespace-nowrap rounded-lg px-3 py-2 text-sm font-semibold transition-colors ${style.action}`}
        to={`${basePath}/subjects/${event.subjectId}?alert=${encodeURIComponent(event.latestAlert?.alertId ?? event.lastDetection?.eventId ?? notice.id)}#alert-history`}
      >
        {notice.kind === "help" ? "대상자 확인" : "상세보기"}
      </Link>
      <button className="grid size-10 place-items-center rounded-lg text-stone-600 hover:bg-stone-100" aria-label={`${subjectName} ${style.label} 알림 닫기`} onClick={onClose}>
        <Icon name="close" />
      </button>
    </motion.aside>
  );
}
