import { motion, useReducedMotion } from "motion/react";
import { useEffect, useRef, useState } from "react";
import SubjectAlertHistory from "./SubjectAlertHistory";
import type { StaffNotice } from "../../utils/staffNotices";
import { responseLabel, riskLabels } from "../../types/monitoring";
import type { Subject } from "../../types/monitoring";
import { formatDateTime, riskBadgeClass, telephoneHref } from "../../utils/format";


const cardClass = "rounded-xl border border-stone-200 bg-white p-6";

export default function SubjectDetail({ subject, history = [], demo = false }: { subject: Subject; history?: StaffNotice[]; demo?: boolean }) {
  const reduceMotion = useReducedMotion();
  const scores = subject.riskTrend?.dailyScores ?? [];
  const chart = useRef<SVGSVGElement>(null);
  const [chartWidth, setChartWidth] = useState(0);
  useEffect(() => {
    if (!chart.current) return;
    const observer = new ResizeObserver(([entry]) => {
      setChartWidth(Math.max(120, entry.contentRect.width));
    });
    observer.observe(chart.current);
    return () => observer.disconnect();
  }, [scores.length]);
  const maximum = Math.max(100, ...scores.map((point) => point.score));
  const points = scores.map((point, index) => ({
    x: 30 + (index * (chartWidth - 60)) / Math.max(1, scores.length - 1),
    y: 150 - (point.score / maximum) * 125,
  }));
  const curve = points.map((point, index) => {
    if (!index) return `M${point.x},${point.y}`;
    const previous = points[index - 1];
    const middle = (previous.x + point.x) / 2;
    return `C${middle},${previous.y} ${middle},${point.y} ${point.x},${point.y}`;
  }).join(" ");
  const chartKey = `${subject.subjectId}:${scores.map((point) => `${point.date}:${point.score}`).join(",")}`;
  return (
    <div className="grid gap-5">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-4">
          <div>
            <h1 className="text-2xl font-bold">
              {subject.name}
              <span className="text-lg font-medium">님</span>
            </h1>
            <p className="mt-1 text-stone-500">{subject.age}세 · {subject.address || "주소 미등록"}</p>
          </div>
        </div>
        <span
          className={`rounded-full px-4 py-2 text-base font-bold ring-1 ${riskBadgeClass(subject.riskLevel)}`}
        >
          {riskLabels[subject.riskLevel]}
        </span>
      </div>
      <div className="grid gap-5 lg:grid-cols-2">
        <section className={cardClass}>
          <h2 className="flex items-center gap-2 text-xl font-bold">
            현재 상태
          </h2>
          <dl className="mt-5 divide-y divide-stone-100">
            {[
              ["위험 점수", `${subject.riskScore}점`],

              ["상태 갱신", formatDateTime(subject.updatedAt)],
            ].map(([term, value]) => (
              <div className="flex justify-between gap-4 py-3" key={term}>
                <dt className="text-stone-500">{term}</dt>
                <dd className="text-right font-semibold">{value}</dd>
              </div>
            ))}
          </dl>
        </section>
        <section className={cardClass}>
          <h2 className="flex items-center gap-2 text-xl font-bold">
            연락 및 알림 응답
          </h2>
          <dl className="mt-5 divide-y divide-stone-100">
            <div className="flex justify-between gap-4 py-3">
              <dt className="text-stone-500">전화번호</dt>
              <dd className="font-semibold">
                {subject.phone ? (
                  <a className="text-brand-700 underline" href={telephoneHref(subject.phone)}>{subject.phone}</a>
                ) : "등록되지 않음"}
              </dd>
            </div>
            <div className="flex justify-between py-3">
              <dt className="text-stone-500">최근 응답</dt>
              <dd className="font-semibold">
                {subject.latestAlert?.managerStatus === "RESOLVED" ? "해결 완료" : responseLabel(subject.latestAlert?.subjectResponse)}
              </dd>
            </div>
            <div className="flex justify-between py-3">
              <dt className="text-stone-500">응답 시각</dt>
              <dd className="font-semibold">
                {formatDateTime(subject.latestAlert?.subjectResponse.respondedAt)}
              </dd>
            </div>
          </dl>
        </section>
      </div>
      <section className={cardClass}>
        <h2 className="flex items-center gap-2 text-xl font-bold">
          일별 위험 점수 추이
          <span className="text-base font-normal text-stone-500">최근 7일</span>
        </h2>
        {scores.length ? (
          <svg
            ref={chart}
            className="mt-4 h-[203px] w-full overflow-visible"
            viewBox={`0 0 ${chartWidth || 640} 203`}
            role="img"
            aria-label={scores.map((point) => `${point.date} ${point.score}점`).join(", ")}
          >
            {chartWidth > 0 && (
              <>
                <path d={`M30 25H${chartWidth - 30}M30 87H${chartWidth - 30}M30 150H${chartWidth - 30}`} stroke="#e7e5e4" />
                <g key={chartKey}>
                  <motion.path
                    d={curve}
                    initial={reduceMotion ? false : { pathLength: 0, opacity: 0 }}
                    animate={{ pathLength: 1, opacity: 1 }}
                    transition={{
                      pathLength: { duration: reduceMotion ? 0 : 1, ease: "linear" },
                      opacity: { duration: reduceMotion ? 0 : 0.1 },
                    }}
                    fill="none"
                    stroke="#f37929"
                    strokeWidth="3"
                    strokeLinecap="round"
                  />
                  {scores.map((point, index) => (
                    <g key={point.date}>
                      <motion.circle
                        cx={points[index].x}
                        cy={points[index].y}
                        r="4"
                        initial={reduceMotion ? false : { opacity: 0 }}
                        animate={{ opacity: 1 }}
                        transition={{
                          duration: reduceMotion ? 0 : 0.15,
                          delay: reduceMotion ? 0 : index / Math.max(1, scores.length - 1),
                        }}
                        fill="#f37929"
                      />
                      <text
                        className="fill-stone-500 text-sm"
                        x={points[index].x}
                        y="176"
                        textAnchor="middle"
                      >
                        {point.date.slice(5)}
                      </text>
                    </g>
                  ))}
                </g>
              </>
            )}
          </svg>
        ) : (
          <p className="mt-5 text-stone-500">아직 기록된 점수 추이가 없습니다.</p>
        )}
      </section>

      <SubjectAlertHistory subject={subject} history={history} demo={demo} />
    </div>
  );
}
