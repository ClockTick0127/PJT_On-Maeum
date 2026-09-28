import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { getApiErrorMessage } from "../../api/client";
import { useSubjectEventsQuery } from "../../hooks/api";
import type { Subject } from "../../types/monitoring";
import { riskLabels } from "../../types/monitoring";
import { formatDateTime, riskBadgeClass, todayInSeoul } from "../../utils/format";
import type { StaffNotice } from "../../utils/staffNotices";
import { alertResponseText, getSubjectAlertHistory } from "../../utils/subjectAlertHistory";

type Props = { subject: Subject; history: StaffNotice[]; demo?: boolean };

export default function SubjectAlertHistory({ subject, history, demo = false }: Props) {
  const [params] = useSearchParams();
  const selectedId = params.get("alert");
  const [days, setDays] = useState(selectedId ? 90 : 30);
  const [to] = useState(todayInSeoul);
  const start = new Date(`${to}T00:00:00Z`);
  start.setUTCDate(start.getUTCDate() - days + 1);
  const from = start.toISOString().slice(0, 10);
  const query = useSubjectEventsQuery(subject.subjectId, subject.version, { from, to, enabled: !demo });
  const events = demo ? [] : query.data?.pages.flatMap((page) => page.events) ?? [];
  const records = getSubjectAlertHistory(subject, events, history);
  const selected = records.find((record) =>
    record.alertId === selectedId || record.eventId === selectedId || record.id === selectedId,
  );
  const selectedKey = selected?.id;
  const selectedElement = useRef<HTMLElement>(null);
  const scrolledTarget = useRef("");
  const { hasNextPage, isFetching, isError, fetchNextPage } = query;

  useEffect(() => {
    if (!selectedId || selected || demo || isFetching || isError || !hasNextPage) return;
    void fetchNextPage();
  }, [selectedId, selected, demo, isFetching, isError, hasNextPage, fetchNextPage]);

  useEffect(() => {
    const target = `${subject.subjectId}:${selectedId}`;
    if (!selectedKey || !selectedElement.current || scrolledTarget.current === target) return;
    selectedElement.current.scrollIntoView({ block: "center" });
    selectedElement.current.focus({ preventScroll: true });
    scrolledTarget.current = target;
  }, [subject.subjectId, selectedId, selectedKey]);

  return (
    <section id="alert-history" className="scroll-mt-6 rounded-xl border border-stone-200 bg-white p-6" aria-labelledby="alert-history-title">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h2 id="alert-history-title" className="text-xl font-bold">알림 및 응답 이력</h2>
        {!demo && (
          <div className="flex flex-wrap items-center gap-2">
            <label className="text-sm text-stone-600" htmlFor="history-period">위험 기록 조회 기간</label>
            <select
              id="history-period"
              value={days}
              onChange={(event) => setDays(Number(event.target.value))}
              className="rounded-lg border border-stone-300 bg-white px-3 py-2 text-base"
            >
              {[7, 30, 90].map((value) => <option key={value} value={value}>최근 {value}일</option>)}
            </select>
            <button
              className="rounded-lg bg-stone-100 px-3 py-2 font-semibold text-stone-700 disabled:opacity-50"
              disabled={query.isFetching}
              onClick={() => void query.refetch()}
            >새로고침</button>
          </div>
        )}
      </div>
      <p className="mt-2 text-sm leading-6 text-stone-500">서버의 위험 기록에 연결된 알림과 응답을 표시합니다. 최신 알림과 현재 화면에서 수신한 실시간 이력도 함께 표시하며, 일부 자동 평가 알림의 과거 이력은 서버 조회에 포함되지 않습니다.</p>
      {query.isError && !demo && (
        <p className="mt-4 rounded-lg bg-red-50 p-4 text-red-700" role="alert">
          위험 기록을 불러오지 못했습니다. {getApiErrorMessage(query.error)} 새로고침으로 다시 시도해 주세요.
        </p>
      )}
      {!demo && query.isLoading && <p className="mt-4 text-stone-500" role="status">서버의 알림 이력을 불러오는 중입니다.</p>}
      {selectedId && !selected && !isFetching && !hasNextPage && !isError && (
        <p className="mt-4 rounded-lg bg-stone-100 p-4 text-stone-700" role="status">
          선택한 알림이 현재 조회 범위에 없습니다. 조회 기간을 늘려 확인해 주세요.
        </p>
      )}
      <div className="mt-5 space-y-4">
        {records.map((record) => {
          const highlighted = record.id === selected?.id;
          const help = record.managerStatus !== "RESOLVED" && record.response?.status === "ANSWERED" && record.response.answer?.toLowerCase() === "yes";
          return (
            <article
              key={record.id}
              ref={highlighted ? selectedElement : undefined}
              tabIndex={highlighted ? -1 : undefined}
              className={`scroll-mt-6 rounded-xl border p-5 outline-offset-4 ${highlighted ? "border-brand-500 bg-orange-50/40" : "border-stone-200"}`}
            >
              <div className="flex flex-wrap items-center justify-between gap-3">
                <h3 className={`text-lg font-bold ${help ? "text-red-700" : "text-stone-800"}`}>{record.alertId ? alertResponseText(record.response) : "위험 이벤트 기록"}</h3>
                {record.managerStatus === "RESOLVED" && <span className="font-semibold text-emerald-700">해결 완료</span>}
                {record.riskLevel && <span className={`rounded-md px-3 py-1 font-semibold ring-1 ${riskBadgeClass(record.riskLevel)}`}>{riskLabels[record.riskLevel]} · {record.riskScore}점</span>}
              </div>
              {record.description && (
                <p className="mt-3 whitespace-pre-wrap break-words leading-7 text-stone-700">{record.description}</p>
              )}
              {record.updates.length > 0 && (
                <ol className="mt-4 space-y-2 border-t border-stone-100 pt-4" aria-label="알림 진행 이력">
                  {record.updates.map((update) => (
                    <li key={update.id} className="flex flex-wrap justify-between gap-x-4 gap-y-1 text-sm">
                      <span className={update.label.startsWith("도움 요청") ? "font-bold text-red-700" : "font-medium text-stone-700"}>
                        {update.label}
                      </span>
                      <time dateTime={update.time} className="text-stone-500">{formatDateTime(update.time)}</time>
                    </li>
                  ))}
                </ol>
              )}
            </article>
          );
        })}
      </div>
      {!records.length && !query.isLoading && !query.isError && <p className="py-8 text-center text-stone-500">조회 가능한 알림 및 응답 이력이 없습니다.</p>}
      {!demo && query.hasNextPage && (
        <button
          className="mt-5 rounded-lg border border-stone-300 px-4 py-3 font-semibold disabled:opacity-50"
          onClick={() => void query.fetchNextPage()}
          disabled={query.isFetchingNextPage}
        >{query.isFetchingNextPage ? "이력을 불러오는 중입니다." : "이력 더 보기"}</button>
      )}
    </section>
  );
}
