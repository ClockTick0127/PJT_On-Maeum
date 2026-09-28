import type { Subject } from "../types/monitoring";

export const LIVE_SUBJECT_ID = "1";

export function applyStaffDemoSubjects(subjects: Subject[]): Subject[] {
  return subjects.map((subject) => {
    if (subject.subjectId === LIVE_SUBJECT_ID) return subject;

    // 시연 대상자 외에는 새로고침과 실시간 수신에도 동일한 임의 점수 유지
    const seed = Array.from(subject.subjectId).reduce(
      (value, character) => (value * 31 + character.charCodeAt(0)) % 997,
      0,
    );
    const warningScore = subject.subjectId === "3" ? 74 : subject.subjectId === "6" ? 81 : null;
    const riskScore = warningScore ?? 12 + seed % 38;

    return {
      ...subject,
      riskScore,
      riskLevel: warningScore === null ? "NORMAL" : "WARNING",
      latestAlert: null,
      riskTrend: {
        ...subject.riskTrend,
        dailyScores: subject.riskTrend.dailyScores.map((point) => ({
          ...point,
          score: riskScore,
        })),
      },
    };
  });
}
