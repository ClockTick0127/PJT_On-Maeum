import { motion, useReducedMotion } from "motion/react";
import type { Subject } from "../../types/monitoring";
import { responseLabel, riskLabels } from "../../types/monitoring";
import { formatDateTime, riskBadgeClass, telephoneHref } from "../../utils/format";
import Icon from "../common/Icon";

export function PreviewDangerAlert({ occurredAt, onClose }: { occurredAt: string; onClose: () => void }) {
  return (
    <aside
      className="fixed top-20 left-1/2 z-50 w-[calc(100%_-_2rem)] max-w-md -translate-x-1/2 rounded-2xl border border-red-200 bg-white p-4 shadow-[0_16px_40px_rgb(127_29_29_/_18%)]"
      role="alert"
    >
      <div className="flex items-start gap-3">
        <span className="grid h-10 w-10 shrink-0 place-items-center rounded-full bg-red-600 text-white">
          <Icon name="bell" />
        </span>
        <div className="min-w-0 flex-1">
          <strong className="block text-red-900">박정수님의 위험 신호가 감지되었습니다.</strong>
          <p className="mt-1 text-base leading-6 text-stone-600">
            92점 · 대상자의 안전 상태를 확인해 주세요.
          </p>
          <time className="mt-1 block text-sm text-stone-500">{formatDateTime(occurredAt)}</time>
        </div>
        <button
          className="grid h-9 w-9 shrink-0 place-items-center rounded-lg text-stone-500 hover:bg-stone-100"
          aria-label="알림 닫기"
          onClick={onClose}
        >
          <Icon name="close" />
        </button>
      </div>
      <button className="mt-3 w-full rounded-xl bg-red-600 px-4 py-2.5 font-semibold text-white hover:bg-red-700">
        상태 확인
      </button>
    </aside>
  );
}

export function PreviewSubjectTable({ subjects }: { subjects: Subject[] }) {
  const reduceMotion = useReducedMotion();
  const sortedSubjects = [...subjects].sort((a, b) => b.riskScore - a.riskScore);

  return (
    <section className="overflow-hidden rounded-xl border border-stone-200/70 bg-white">
      <div className="border-b border-stone-100 p-5">
        <h2 className="flex items-center gap-2 text-xl font-bold">
          <Icon name="users" /> 등록 대상자
        </h2>
        <p className="mt-1 text-base text-stone-500">위험 점수가 높은 순서로 표시됩니다.</p>
      </div>
      <motion.div layoutScroll className="overflow-x-auto">
        <table className="w-full min-w-4xl text-left">
          <thead className="bg-stone-50 text-base text-stone-500">
            <tr>
              <th className="p-4">확인 순서 · 대상자</th>
              <th>전화번호</th>
              <th>위험 단계</th>
              <th>현재 위험 점수</th>
              <th>최근 알림 응답</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-stone-100">
            {sortedSubjects.map((subject, index) => (
              <motion.tr
                layout={reduceMotion ? false : "position"}
                transition={{ layout: { type: "spring", stiffness: 120, damping: 24, mass: 1 } }}
                className={subject.riskLevel === "DANGER" ? "bg-red-50" : "hover:bg-stone-50"}
                key={subject.subjectId}
              >
                <td className="p-4">
                  <strong><span className="mr-3 text-base font-medium text-stone-400">{index + 1}</span>{subject.name} · {subject.age}세</strong>
                  <small className="mt-1 block text-[0.9375rem] leading-6 text-stone-500">{subject.address || "주소 미등록"}</small>
                </td>
                <td>
                  {subject.phone ? (
                    <a className="text-base text-stone-600 hover:text-brand-700 hover:underline" href={telephoneHref(subject.phone)}>{subject.phone}</a>
                  ) : "미등록"}
                </td>
                <td>
                  <span className={`rounded-full px-3 py-1 text-sm font-bold ring-1 ${riskBadgeClass(subject.riskLevel)}`}>
                    {riskLabels[subject.riskLevel]}
                  </span>
                </td>
                <td className="font-bold">{subject.riskScore}점</td>
                <td>{responseLabel(subject.latestAlert?.subjectResponse)}</td>
              </motion.tr>
            ))}
          </tbody>
        </table>
      </motion.div>
      <footer className="border-t border-stone-100 p-4 text-base text-stone-500">
        총 {subjects.length}명 표시
      </footer>
    </section>
  );
}
