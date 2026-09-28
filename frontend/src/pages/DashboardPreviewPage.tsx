import { useState } from "react";
import Brand from "../components/common/Brand";
import Icon from "../components/common/Icon";
import OverviewMetrics from "../components/staff/OverviewMetrics";
import { PreviewDangerAlert, PreviewSubjectTable } from "../components/staff/DashboardPreviewContent";
import type { Subject } from "../types/monitoring";

const baseSubjects: Subject[] = [
  ["1", "김영희", 78, "서울시 중구 다산로", "010-1234-1001", "NORMAL", 18, "냉장고"],
  ["2", "박정수", 74, "서울시 중구 퇴계로", "010-1234-1002", "WARNING", 62, "전기밥솥"],
  ["3", "이순자", 81, "서울시 중구 동호로", "010-1234-1003", "NORMAL", 24, "TV"],
  ["4", "최명자", 69, "서울시 중구 청구로", "010-1234-1004", "NORMAL", 12, "세탁기"],
  ["5", "정영호", 76, "서울시 중구 을지로", "010-1234-1005", "WARNING", 55, "전자레인지"],
].map(([subjectId, name, age, address, phone, riskLevel, riskScore, applianceType], index) => ({
  subjectId: String(subjectId),
  name: String(name),
  age: Number(age),
  address: String(address),
  phone: String(phone),
  version: 1,
  riskLevel: riskLevel as Subject["riskLevel"],
  riskScore: Number(riskScore),
  updatedAt: new Date(Date.now() - index * 12 * 60_000).toISOString(),
  lastActivity: {
    occurredAt: new Date(Date.now() - (index + 1) * 18 * 60_000).toISOString(),
    applianceType: String(applianceType),
  },
  latestAlert: null,
  riskTrend: { timezone: "Asia/Seoul", dailyScores: [] },
}));

export default function DashboardPreviewPage({ danger = false }: { danger?: boolean }) {
  const alertAt = new Date().toISOString();
  const [alertVisible, setAlertVisible] = useState(danger);
  const subjects = danger
    ? baseSubjects.map((subject, index) =>
        index === 1
          ? {
              ...subject,
              version: subject.version + 1,
              riskLevel: "DANGER" as const,
              riskScore: 92,
              updatedAt: alertAt,
              latestAlert: {
                alertId: "preview-alert",
                eventId: "preview-event",
                subjectResponse: { status: "PENDING", answer: null, respondedAt: null },
              },
            }
          : subject,
      )
    : baseSubjects;

  return (
    <main className="min-h-screen bg-[#fcfcfb] text-stone-800 lg:grid lg:grid-cols-[250px_1fr]">
      <aside className="flex items-center gap-4 border-b border-stone-200/70 bg-[#fcfcfb] p-4 lg:fixed lg:inset-y-0 lg:w-[250px] lg:flex-col lg:items-stretch lg:border-b-0 lg:p-7">
        <Brand />
        <nav className="ml-auto flex gap-1 lg:mt-14 lg:ml-0 lg:grid lg:gap-2" aria-label="담당자 메뉴">
          <span className="flex items-center gap-3 rounded-lg bg-brand-500 px-4 py-3.5 text-base font-semibold text-white ">
            <Icon name="grid" />
            <span className="hidden lg:inline">대시보드</span>
          </span>
          <span className="flex items-center gap-3 rounded-lg px-4 py-3.5 text-base font-semibold text-stone-400">
            <Icon name="users" />
            <span className="hidden lg:inline">대상자 관리</span>
          </span>
          <span className="flex items-center gap-3 rounded-lg px-4 py-3.5 text-base font-semibold text-stone-400">
            <Icon name="bell" />
            <span className="hidden lg:inline">최근 알림</span>
          </span>
        </nav>
        <div className="mt-auto hidden border-t border-stone-100 pt-5 lg:block">
          <p className="font-bold">복지담당자</p>
          <p className="text-base text-stone-500">함께 지키는 안심 일상</p>
        </div>
      </aside>

      <div className="m-3 ml-0 min-w-0 rounded-[1.75rem] border border-stone-200/80 bg-[#f5f5f4] lg:col-start-2 lg:m-6 lg:ml-0 lg:rounded-[2rem]">
        <header className="rounded-t-[1.75rem] lg:rounded-t-[2rem] flex min-h-20 items-center justify-between border-b border-stone-200 bg-[#f5f5f4] px-5 md:px-10">
          <strong>대시보드</strong>
          <span className="flex items-center gap-2 text-sm text-stone-500">
             실시간 연결됨
          </span>
        </header>

        <div className="mx-auto grid max-w-[1440px] gap-6 p-5 md:p-8 xl:p-10">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <h1 className="text-2xl font-bold tracking-tight md:text-[1.875rem]">오늘의 돌봄 현황</h1>

            </div>
            <button
              className="flex items-center gap-2 rounded-lg bg-brand-500 px-5 py-3 text-base font-semibold text-white hover:bg-brand-600"
              type="button"
            >
              대상자 등록
            </button>
          </div>

          {alertVisible && (
            <PreviewDangerAlert occurredAt={alertAt} onClose={() => setAlertVisible(false)} />
          )}

          <OverviewMetrics subjects={subjects} loading={false} />

          <PreviewSubjectTable subjects={subjects} />
        </div>
      </div>
    </main>
  );
}
