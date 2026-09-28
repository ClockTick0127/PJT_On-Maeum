import { useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import OverviewMetrics from "../components/staff/OverviewMetrics";
import RecentAlerts from "../components/staff/RecentAlerts";
import StaffHeader from "../components/staff/StaffHeader";
import StaffSidebar from "../components/staff/StaffSidebar";
import { staffNavigation } from "../components/staff/staffNavigation";
import type { StaffView } from "../components/staff/staffNavigation";
import StaffNotifications from "../components/staff/StaffNotifications";
import SubjectDetail from "../components/staff/SubjectDetail";
import SubjectList from "../components/staff/SubjectList";
import type { StatusEvent, Subject } from "../types/monitoring";
import { addStaffNotice, createStaffNotice } from "../utils/staffNotices";
import type { StaffNotice } from "../utils/staffNotices";

const basePath = "/demo/dashboard";
const profiles = [
  { name: "김영희", age: 78, address: "서울시 중구 다산로 · 가상 주소" },
  { name: "박정수", age: 74, address: "서울시 중구 퇴계로 · 가상 주소" },
  { name: "이순자", age: 81, address: "서울시 중구 동호로 · 가상 주소" },
  { name: "최명자", age: 69, address: "서울시 중구 청구로 · 가상 주소" },
  { name: "정영호", age: 76, address: "서울시 중구 을지로 · 가상 주소" },
];
const scenarios = [
  { label: "평상시", scores: [18, 24, 12, 31, 9], description: "모든 대상자가 안정 상태입니다. 화면 알림을 초기화합니다.", response: null, answer: null },
  { label: "고위험 점수", scores: [43, 96, 71, 58, 22], description: "박정수님의 점수가 상승해 확인 순서가 가장 앞으로 이동합니다.", response: null, answer: null },
  { label: "위험 알림 발생", scores: [43, 96, 71, 58, 22], description: "자동 감지된 위험 알림입니다. 이어서 도움 요청 응답을 누르면 두 알림이 별도로 표시됩니다.", response: "PENDING", answer: null },
  { label: "도움 요청 응답", scores: [43, 98, 71, 58, 22], description: "대상자가 직접 도움을 요청했습니다. 앞서 발생한 위험 감지 알림과 구분하여 표시됩니다.", response: "ANSWERED", answer: "YES" },
  { label: "괜찮아요 응답", scores: [43, 96, 71, 58, 22], description: "안전 응답은 도움 요청으로 표시하지 않습니다. 점수와 위험 단계는 서버 값과 독립적으로 유지합니다.", response: "ANSWERED", answer: "NO" },
  { label: "응답 시간 만료", scores: [43, 96, 71, 58, 22], description: "응답 시간 만료를 도움 요청과 별도 안내로 표시합니다.", response: "EXPIRED", answer: null },
] as const;

function createSnapshot(scenarioIndex: number) {
  const scenario = scenarios[scenarioIndex];
  const now = new Date();
  const updatedAt = now.getTime();
  const subjects: Subject[] = profiles.map((profile, index) => {
    const score = scenario.scores[index];
    return {
      ...profile,
      subjectId: `dashboard-demo-${index + 1}`,
      phone: "010-0000-0000",
      version: updatedAt,
      riskScore: score,
      riskLevel: score >= 80 ? "DANGER" : score >= 50 ? "WARNING" : "NORMAL",
      updatedAt: now.toISOString(),
      lastActivity: null,
      latestAlert: index === 1 && scenario.response ? {
        alertId: "dashboard-demo-alert",
        eventId: "dashboard-demo-event",
        subjectResponse: {
          status: scenario.response,
          answer: scenario.answer,
          respondedAt: scenario.response === "ANSWERED" ? now.toISOString() : null,
        },
      } : null,
      riskTrend: {
        timezone: "Asia/Seoul",
        dailyScores: [16, 23, 19, 36, 29, 48, score].map((value, day) => {
          const date = new Date(now);
          date.setDate(date.getDate() - 6 + day);
          return {
            date: `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`,
            score: day === 6 ? score : Math.min(value + index * 3, score),
          };
        }),
      },
    };
  });
  const alerted = subjects[1];
  const notice: StatusEvent | null = scenario.response ? {
    subjectId: alerted.subjectId,
    version: alerted.version,
    riskLevel: alerted.riskLevel,
    riskScore: alerted.riskScore,
    updatedAt: alerted.updatedAt,
    lastActivity: null,
    latestAlert: alerted.latestAlert,
    trigger: scenario.response === "ANSWERED" ? "SUBJECT_RESPONSE" : scenario.response === "EXPIRED" ? "RESPONSE_EXPIRED" : "DETECTION",
    recentEventCount: 1,
    unresolvedAlertCount: 1,
    lastDetection: null,
  } : null;
  return { subjects, notice, updatedAt };
}

export default function DashboardDemoPage() {
  const navigate = useNavigate();
  const { subjectId } = useParams();
  const [searchParams] = useSearchParams();
  const [scenarioIndex, setScenarioIndex] = useState(1);
  const [snapshot, setSnapshot] = useState(() => createSnapshot(1));
  const [notices, setNotices] = useState<StaffNotice[]>([]);
  const [history, setHistory] = useState<StaffNotice[]>([]);
  const requestedView = searchParams.get("view");
  const view: StaffView = staffNavigation.some((item) => item.key === requestedView)
    ? requestedView as StaffView
    : "overview";
  const selected = snapshot.subjects.find((subject) => subject.subjectId === subjectId);
  const showNotifications = !subjectId && view === "overview";
  const title = subjectId ? "대상자 상세" : staffNavigation.find((item) => item.key === view)?.label;

  const applyScenario = (index: number) => {
    const next = createSnapshot(index);
    setScenarioIndex(index);
    setSnapshot(next);
    if (!next.notice) {
      setNotices([]);
      if (index === 0) setHistory([]);
      return;
    }
    const notice = createStaffNotice(next.notice);
    if (notice) {
      setNotices((current) => addStaffNotice(current, notice));
      setHistory((current) => addStaffNotice(current, notice));
    }
  };

  return (
    <main className="min-h-screen bg-[#fcfcfb] text-stone-800 lg:grid lg:grid-cols-[250px_1fr]">
      <StaffSidebar
        view={view}
        subjectId={subjectId}
        basePath={basePath}
        manager={{ name: "데모 담당자", email: "demo@example.com" }}
        onLogout={() => navigate("/")}
      />
      <div className="m-3 ml-0 min-w-0 rounded-[1.75rem] border border-stone-200/80 bg-[#f5f5f4] lg:col-start-2 lg:m-6 lg:ml-0 lg:rounded-[2rem]">
        <StaffHeader title={title} connection="connected" refreshing={false} onRefresh={() => applyScenario(scenarioIndex)} />
        <div className={`mx-auto grid max-w-[1440px] gap-6 p-5 md:p-8 xl:p-10 ${showNotifications && notices.length ? "pb-32 md:pb-32 xl:pb-32" : ""}`}>
          <section className="rounded-xl border border-stone-200 bg-white p-5" aria-label="대시보드 데모 설정">
            <div className="flex flex-wrap items-baseline justify-between gap-3">
              <h1 className="text-xl font-bold">대시보드 디자인 데모</h1>
              <Link className="text-sm font-semibold text-stone-600 underline" to="/demo/priority">순위 이동 데모</Link>
            </div>
            <p className="mt-2 text-sm leading-6 text-stone-500">로그인 없이 확인하는 가상 데이터 화면입니다. 실제 알림이나 서버 요청은 발생하지 않습니다.</p>
            <div className="mt-4 flex flex-wrap gap-2">
              {scenarios.map((scenario, index) => (
                <button
                  key={scenario.label}
                  type="button"
                  aria-pressed={scenarioIndex === index}
                  className={`min-h-11 rounded-lg border px-4 py-2 font-semibold transition-colors ${scenarioIndex === index ? "border-brand-500 bg-brand-500 text-white" : "border-stone-300 bg-white text-stone-700 hover:bg-stone-50"}`}
                  onClick={() => applyScenario(index)}
                >
                  {scenario.label}
                </button>
              ))}
            </div>
            <p className="mt-4 leading-6 text-stone-600" role="status">{scenarios[scenarioIndex].description}</p>
          </section>

          {showNotifications && <StaffNotifications
            notices={notices}
            subjects={snapshot.subjects}
            basePath={basePath}
            onDismiss={(id) => setNotices((current) => current.filter((notice) => notice.id !== id))}
          />}

          {subjectId ? (
            <>
              <Link className="w-fit font-semibold text-brand-700" to={`${basePath}?view=subjects`}>대상자 목록으로</Link>
              {selected ? <SubjectDetail subject={selected} history={history} demo /> : <p>데모 대상자를 찾을 수 없습니다.</p>}
            </>
          ) : (
            <>
              <h2 className="text-2xl font-bold tracking-tight md:text-[1.875rem]">{view === "overview" ? "대상자 현황" : title}</h2>
              {view === "overview" && <OverviewMetrics subjects={snapshot.subjects} loading={false} />}
              {view === "alerts" ? (
                <RecentAlerts subjects={snapshot.subjects} history={history} basePath={basePath} />
              ) : (
                <SubjectList subjects={snapshot.subjects} loading={false} dataUpdatedAt={snapshot.updatedAt} basePath={basePath} />
              )}
            </>
          )}
        </div>
      </div>
    </main>
  );
}
