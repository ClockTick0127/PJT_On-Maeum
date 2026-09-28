import { motion, useReducedMotion } from "motion/react";
import { Link } from "react-router-dom";
import type { Subject } from "../../types/monitoring";
import { getRecentAlerts } from "../../utils/staffNotices";
import type { RecentAlertItem, StaffNotice } from "../../utils/staffNotices";
import { formatDateTime } from "../../utils/format";

const content: Record<RecentAlertItem["kind"], { label: string; description: string }> = {
  risk: { label: "위험 감지", description: "위험 신호가 감지되었습니다. 연락하여 안부를 확인해 주세요." },
  help: { label: "도움 요청", description: "‘도움이 필요해요’ 응답이 도착했습니다. 바로 연락해 주세요." },
  safe: { label: "안전 응답", description: "대상자가 ‘괜찮아요’라고 응답했습니다." },
  expired: { label: "응답 시간 만료", description: "안부 확인에 응답하지 않았습니다. 연락하여 상태를 확인해 주세요." },
  "check-in": { label: "안부 확인 알림", description: "서버에 등록된 최근 안부 확인 알림입니다. 대상자의 응답 상태를 확인해 주세요." },
};

type RecentAlertsProps = {
  subjects: Subject[];
  history?: StaffNotice[];
  loading?: boolean;
  basePath?: string;
};

export default function RecentAlerts({ subjects, history = [], loading = false, basePath = "/staff" }: RecentAlertsProps) {
  const alerts = getRecentAlerts(subjects, history);

  const reduceMotion = useReducedMotion();

  return (
    <motion.section
      initial={reduceMotion ? false : { opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, ease: "easeOut" }}
      className="rounded-xl border border-stone-200/70 bg-white p-7"
    >
      <h2 className="flex items-center gap-2 text-xl font-bold">
        최근 알림 내역
      </h2>
      <p className="mt-2 text-sm leading-6 text-stone-500">이 화면에서 수신한 알림과 서버의 대상자별 최신 알림을 최근 순서로 표시합니다.</p>
      <div className="mt-5 divide-y divide-stone-100">
        {alerts.map((alert) => (
          <motion.article
            layout={reduceMotion ? false : "position"}
            transition={{ layout: { type: "spring", stiffness: 120, damping: 24, mass: 1 } }}
            className="flex flex-wrap items-center gap-4 py-4" key={alert.id}
          >
            <div className="min-w-48 flex-1">
              <div className="flex flex-wrap items-center gap-3">
                <strong>{subjects.find((subject) => subject.subjectId === alert.subjectId)?.name ?? "대상자"}님</strong>
                <span className={alert.resolved ? "font-semibold text-stone-500" : alert.kind === "help" ? "rounded-md bg-red-600 px-2.5 py-1 text-base font-bold leading-5 text-white" : `font-semibold ${alert.kind === "risk" ? "text-orange-800" : alert.kind === "safe" ? "text-emerald-700" : "text-stone-700"}`}>
                  {content[alert.kind].label}
                  {alert.resolved && " · 해결 완료"}
                </span>
                <span className="font-semibold tabular-nums text-stone-700">{alert.score}점</span>
              </div>
              <p className="mt-2 text-sm leading-6 text-stone-600">{alert.resolved ? "담당자가 조치를 완료한 알림입니다." : content[alert.kind].description}</p>
              <time className="mt-1 block text-sm text-stone-500" dateTime={alert.time}>
                {alert.source === "dashboard" ? "서버 최신 정보 · " : "수신 시각 · "}{formatDateTime(alert.time)}
              </time>
            </div>
            <Link
              className="inline-flex min-h-10 items-center rounded-lg border border-stone-300 bg-white px-4 py-2 font-semibold text-stone-800 hover:bg-stone-50"
              to={`${basePath}/subjects/${alert.subjectId}?alert=${encodeURIComponent(alert.targetId)}#alert-history`}
            >
              상세보기
            </Link>
          </motion.article>
        ))}
      </div>
      {!alerts.length && (
        <p className="py-10 text-center text-stone-500">{loading ? "최근 알림을 불러오는 중입니다." : "조회된 최근 알림이 없습니다."}</p>
      )}
    </motion.section>
  );
}
