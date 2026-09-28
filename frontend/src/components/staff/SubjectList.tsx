import { motion, useReducedMotion } from "motion/react";
import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import type { Risk, Subject } from "../../types/monitoring";
import { responseLabel, riskLabels } from "../../types/monitoring";
import { formatDateTime, riskBadgeClass, telephoneHref } from "../../utils/format";
import Icon from "../common/Icon";
import { isHelpRequested } from "../../utils/staffNotices";

type SubjectListProps = {
  subjects: Subject[];
  loading: boolean;
  dataUpdatedAt: number;
  detailLinks?: boolean;
  basePath?: string;
  onResolve?: (alertId: string) => void;
  resolving?: boolean;
};

export default function SubjectList({ subjects, loading, dataUpdatedAt, detailLinks = true, basePath = "/staff", onResolve, resolving = false }: SubjectListProps) {
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<"ALL" | Risk>("ALL");
  const visible = useMemo(
    () =>
      subjects
        .filter(
          (subject) =>
            `${subject.name} ${subject.address} ${subject.phone}`
              .toLowerCase()
              .includes(search.trim().toLowerCase()) &&
            (filter === "ALL" || subject.riskLevel === filter),
        )
        .sort((a, b) => b.riskScore - a.riskScore),
    [subjects, search, filter],
  );

  const filters = [
    { id: "ALL" as const, label: "전체" },
    ...(["DANGER", "WARNING", "NORMAL"] as Risk[]).map((risk) => ({
      id: risk,
      label: riskLabels[risk],
    })),
  ];

  const reduceMotion = useReducedMotion();

  return (
    <motion.section
      initial={reduceMotion ? false : { opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, ease: "easeOut" }}
      className="overflow-hidden rounded-xl border border-stone-200/70 bg-white"
    >
      <div className="flex flex-wrap items-center justify-between gap-3 px-6 pt-6">
        <h2 className="text-lg font-bold tracking-tight">등록 대상자</h2>
      </div>
      <div className="flex flex-wrap items-center gap-4 border-b border-stone-100 p-5 md:px-6">
        <div className="flex flex-wrap gap-2">
          {filters.map((item) => (
            <button
              className={`min-h-10 rounded-lg border px-5 py-2 text-base font-semibold transition-colors ${filter === item.id ? "border-brand-500 bg-brand-500 text-white" : "border-stone-200 bg-white text-stone-600 hover:border-stone-300 hover:bg-stone-50 hover:text-stone-900"}`}
              key={item.id}
              aria-pressed={filter === item.id}
              onClick={() => setFilter(item.id)}
            >
              {item.label}
            </button>
          ))}
        </div>
        <label className="ml-auto flex min-w-64 flex-1 items-center gap-2 rounded-lg border border-stone-200/70 bg-stone-50 px-4 md:max-w-sm">
          <Icon className="text-stone-400" name="search" />
          <input
            className="h-11 min-w-0 flex-1 bg-transparent outline-none"
            aria-label="대상자 검색"
            placeholder="이름, 주소, 전화번호 검색"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
        </label>
      </div>
      <motion.div layoutScroll className="overflow-x-auto">
        <table className="w-full min-w-4xl text-left">
          <thead className="bg-stone-50 text-base text-stone-500">
            <tr>
              <th className="px-6 py-5">확인 순서 · 대상자</th>
              <th>전화번호</th>
              <th>위험 단계</th>
              <th>현재 위험 점수</th>
              <th>최근 알림 응답</th>
              {detailLinks && <th>상세</th>}
            </tr>
          </thead>
          <tbody className="divide-y divide-stone-100">
            {visible.map((subject, index) => (
              <motion.tr
                layout={reduceMotion ? false : "position"}
                initial={reduceMotion ? false : { opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{
                  opacity: { duration: 0.2, delay: reduceMotion ? 0 : Math.min(index, 5) * 0.035 },
                  layout: { type: "spring", stiffness: 120, damping: 24, mass: 1 },
                }}
                className={`transition-colors ${subject.riskLevel === "DANGER" ? "bg-red-50/80 hover:bg-red-100/70" : "hover:bg-stone-50"}`}
                key={subject.subjectId}
              >
                <td className={`border-l-[3px] p-4 ${subject.riskLevel === "DANGER" ? "border-l-red-500" : "border-l-transparent"}`}>
                  <strong>
                    <span className="mr-3 text-base font-medium tabular-nums text-stone-400">{index + 1}</span>
                    {subject.name} · {subject.age}세
                  </strong>
                  {isHelpRequested(subject.latestAlert) && <span className="ml-3 inline-block whitespace-nowrap rounded-md bg-red-600 px-2.5 py-1 text-base leading-5 font-bold text-white">도움 요청</span>}
                  <small className="mt-1 block text-[0.9375rem] leading-6 text-stone-500">{subject.address || "주소 미등록"}</small>
                </td>
                <td>
                  {subject.phone ? (
                    <a className="text-base text-stone-600 hover:text-brand-700 hover:underline" href={telephoneHref(subject.phone)}>{subject.phone}</a>
                  ) : "미등록"}
                </td>
                <td>
                  <span
                    className={`rounded-full px-3 py-1 text-sm font-bold ring-1 ${riskBadgeClass(subject.riskLevel)}`}
                  >
                    {riskLabels[subject.riskLevel]}
                  </span>
                </td>
                <td className={`font-bold tabular-nums ${subject.riskLevel === "DANGER" ? "text-red-700" : ""}`}>{subject.riskScore}점</td>
                <td>
                  {subject.latestAlert?.managerStatus === "RESOLVED" ? "해결 완료" : responseLabel(subject.latestAlert?.subjectResponse)}
                  {onResolve && isHelpRequested(subject.latestAlert) && subject.latestAlert && (
                    <button
                      type="button"
                      className="ml-3 rounded-lg border border-brand-300 px-3 py-2 text-sm font-semibold text-brand-700 disabled:opacity-50"
                      disabled={resolving}
                      onClick={() => onResolve(subject.latestAlert!.alertId)}
                      aria-label={`${subject.name} 도움 요청 해결 완료`}
                    >해결 완료</button>
                  )}
                </td>
                {detailLinks && <td>
                  <Link
                    className="inline-flex min-h-10 items-center justify-center gap-2 whitespace-nowrap rounded-lg border border-stone-300 bg-white px-4 py-2 text-base font-semibold text-stone-800 transition-colors hover:border-brand-400 hover:bg-brand-50 hover:text-stone-900"
                    to={`${basePath}/subjects/${subject.subjectId}`}
                  >
                    상세보기
                  </Link>
                </td>}
              </motion.tr>
            ))}
          </tbody>
        </table>
      </motion.div>
      {!visible.length && (
        <p className="py-12 text-center text-stone-500">
          {loading ? "대상자를 불러오는 중입니다." : "조건에 맞는 대상자가 없습니다."}
        </p>
      )}
      <footer className="flex justify-between border-t border-stone-100 px-6 py-4 text-sm text-stone-500">
        <span>{visible.length}명</span>
        <span>
          최근 갱신{" "}
          {dataUpdatedAt ? formatDateTime(new Date(dataUpdatedAt).toISOString()) : "기록 없음"}
        </span>
      </footer>
    </motion.section>
  );
}
