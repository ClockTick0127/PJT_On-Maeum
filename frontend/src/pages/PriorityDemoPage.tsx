import { useEffect, useState } from "react";
import { useReducedMotion } from "motion/react";
import { Link } from "react-router-dom";
import SubjectList from "../components/staff/SubjectList";
import type { Subject } from "../types/monitoring";

const names = ["김영희", "박정수", "이순자", "최명자", "정영호", "한미숙"];
const scenarios = [
  { label: "처음 순서", scores: [88, 72, 56, 43, 28, 15] },
  { label: "박정수님이 1순위로", scores: [68, 96, 56, 43, 28, 15] },
  { label: "한미숙님이 마지막에서 처음으로", scores: [68, 76, 56, 43, 28, 99] },
  { label: "여러 대상자의 순위 변경", scores: [22, 51, 91, 83, 65, 36] },
];

function createSubjects(scores: number[]): Subject[] {
  return names.map((name, index) => ({
    subjectId: `priority-demo-${index}`,
    name,
    age: 70 + index,
    address: "데모용 주소",
    phone: "",
    version: 1,
    riskScore: scores[index],
    riskLevel: scores[index] >= 80 ? "DANGER" : scores[index] >= 50 ? "WARNING" : "NORMAL",
    updatedAt: new Date().toISOString(),
    lastActivity: null,
    latestAlert: null,
    riskTrend: { timezone: "Asia/Seoul", dailyScores: [] },
  }));
}

export default function PriorityDemoPage() {
  const [scenario, setScenario] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [subjects, setSubjects] = useState(() => createSubjects(scenarios[0].scores));
  const [updatedAt, setUpdatedAt] = useState(() => Date.now());
  const reduceMotion = useReducedMotion();

  useEffect(() => {
    if (!playing) return;
    const timer = window.setInterval(() => {
      setScenario((current) => {
        const next = (current + 1) % scenarios.length;
        return next;
      });
    }, 3500);
    return () => window.clearInterval(timer);
  }, [playing]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setSubjects(createSubjects(scenarios[scenario].scores));
      setUpdatedAt(Date.now());
    }, 0);
    return () => window.clearTimeout(timer);
  }, [scenario]);

  return (
    <main className="min-h-screen bg-[#fcfcfb] p-4 text-stone-800 md:p-8">
      <div className="mx-auto grid max-w-6xl gap-6 rounded-[2rem] border border-stone-200 bg-stone-50 p-5 md:p-8">
        <header className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold">대상자 우선순위 이동 데모</h1>
            <p className="mt-2 text-sm leading-relaxed text-stone-600">가상 대상자 6명의 점수가 바뀌면 실제 대시보드 목록이 새 순위로 이동합니다.</p>
          </div>
          <Link className="text-sm font-semibold text-brand-700 hover:underline" to="/demo/dashboard">대시보드 미리보기</Link>
        </header>
        <section className="rounded-2xl border border-stone-200 bg-white p-5" aria-label="데모 제어">
          <div className="flex flex-wrap gap-3">
            <button className="min-h-11 rounded-full bg-brand-500 px-5 py-2 font-semibold text-white hover:bg-brand-600" type="button" aria-pressed={playing} onClick={() => setPlaying((value) => !value)}>
              {playing ? "자동 재생 멈추기" : "자동 재생하기"}
            </button>
            <button className="min-h-11 rounded-full border border-stone-300 px-5 py-2 font-semibold hover:bg-stone-50" type="button" onClick={() => { setPlaying(false); setScenario((current) => (current + 1) % scenarios.length); }}>
              다음 순위 보기
            </button>
            <button className="min-h-11 rounded-full border border-stone-300 px-5 py-2 font-semibold hover:bg-stone-50" type="button" onClick={() => { setPlaying(false); setScenario(0); }}>
              처음으로
            </button>
          </div>
          <p className="mt-4 font-semibold" role="status">{scenarios[scenario].label}</p>
          <p className="mt-2 text-sm text-stone-500">자동 재생은 3.5초마다 점수를 변경합니다. 검색과 필터를 비우면 6명 전체의 이동을 볼 수 있어요.</p>
          {reduceMotion && <p className="mt-3 text-sm font-semibold text-brand-700">기기의 ‘동작 줄이기’ 설정이 켜져 있어 이동 효과가 생략됩니다.</p>}
        </section>
        <SubjectList subjects={subjects} loading={false} dataUpdatedAt={updatedAt} detailLinks={false} />
        <p className="text-xs text-stone-500">점수와 위험 단계는 애니메이션 확인용 가상 값입니다. 서버 요청과 실제 대상자 데이터 변경은 없습니다.</p>
      </div>
    </main>
  );
}
