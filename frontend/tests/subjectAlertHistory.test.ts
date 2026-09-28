import { describe, expect, it } from "vitest";
import type { Events, StatusEvent, Subject } from "../src/types/monitoring";
import { createStaffNotice } from "../src/utils/staffNotices";
import { alertResponseText, getSubjectAlertHistory } from "../src/utils/subjectAlertHistory";

const response = { status: "ANSWERED", answer: "YES", respondedAt: "2026-09-26T10:01:00Z" };
const subject: Subject = {
  subjectId: "1", name: "박정수", age: 74, address: "", phone: "", version: 3,
  riskLevel: "DANGER", riskScore: 96, updatedAt: "2026-09-26T10:02:00Z", lastActivity: null,
  latestAlert: { alertId: "a1", eventId: "e1", managerStatus: "UNCONFIRMED", subjectResponse: response },
  riskTrend: { timezone: "Asia/Seoul", dailyScores: [] },
};
const event: Events["events"][number] = {
  eventId: "e1", eventType: "RISK", description: "위험 신호 감지 내용", riskLevel: "DANGER", riskScore: 95,
  occurredAt: "2026-09-26T10:00:00Z", reason: { severity: "high" },
  alert: { alertId: "a1", managerStatus: "ACKNOWLEDGED", managerStatusUpdatedAt: "2026-09-26T10:03:00Z", subjectResponse: { status: "PENDING", answer: null, respondedAt: null } },
};
function notice(answer: string, time: string) {
  const status: StatusEvent = {
    ...subject, updatedAt: time, trigger: "SUBJECT_RESPONSE", recentEventCount: 1, unresolvedAlertCount: 1, lastDetection: null,
    latestAlert: { ...subject.latestAlert!, subjectResponse: { status: "ANSWERED", answer, respondedAt: time } },
  };
  return createStaffNotice(status)!;
}

describe("대상자 알림 및 응답 이력", () => {
  it("서버 위험 기록, 실시간 도움 요청, 최신 알림을 중복 없이 합치고 모든 세부 정보를 유지한다", () => {
    const rows = getSubjectAlertHistory(subject, [event], [notice("YES", response.respondedAt)]);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      description: event.description, reason: event.reason, riskScore: 95, response,
      managerStatus: "ACKNOWLEDGED", managerStatusUpdatedAt: event.alert!.managerStatusUpdatedAt,
    });
    expect(rows[0].updates.map((update) => update.label)).toEqual(["위험 이벤트 기록", "도움 요청 · 도움이 필요해요"]);
  });

  it("같은 알림의 서로 다른 응답을 시간순으로 유지하고 오래된 최신 스냅샷이 새 응답을 덮지 않는다", () => {
    const rows = getSubjectAlertHistory(subject, [event], [notice("NO", "2026-09-26T10:04:00Z"), notice("YES", response.respondedAt)]);
    expect(rows[0].response?.answer).toBe("NO");
    expect(rows[0].updates.map((update) => update.label)).toEqual(["위험 이벤트 기록", "도움 요청 · 도움이 필요해요", "안전 응답 · 괜찮아요"]);
  });

  it("이벤트가 없는 자동 평가 알림도 표시하고 다른 대상자 기록은 제외한다", () => {
    const help = notice("YES", response.respondedAt);
    const rows = getSubjectAlertHistory({ ...subject, latestAlert: { ...subject.latestAlert!, eventId: null } }, [], [help, { ...help, event: { ...help.event, subjectId: "2" } }]);
    expect(rows).toHaveLength(1);
    expect(rows[0].source).toBe("stream");
    expect(rows[0].occurredAt).toBeUndefined();
  });

  it("서버 최신 알림의 현재 위험 점수를 과거 발생 점수로 표시하지 않는다", () => {
    const rows = getSubjectAlertHistory(subject, [], []);
    expect(rows[0].source).toBe("latest");
    expect(rows[0].riskScore).toBeUndefined();
    expect(rows[0].occurredAt).toBeUndefined();
  });

  it("연결된 알림이 없는 위험 이벤트도 유지하고 최신 응답 순서로 정렬한다", () => {
    const rows = getSubjectAlertHistory(subject, [event, { ...event, eventId: "e2", alert: null, occurredAt: "2026-09-26T10:02:00Z" }], []);
    expect(rows).toHaveLength(2);
    expect(rows.map((row) => row.id)).toEqual(["a1", "e2"]);
  });

  it("자동 평가 알림에 무관한 마지막 감지 이벤트를 연결하지 않는다", () => {
    const risk = createStaffNotice({ ...subject, trigger: "ASSESSMENT", recentEventCount: 1, unresolvedAlertCount: 1, lastDetection: { eventId: "other", description: "무관한 이벤트", occurredAt: event.occurredAt } })!;
    const rows = getSubjectAlertHistory(subject, [], [risk]);
    expect(rows[0].description).toBeUndefined();
    expect(rows[0].occurredAt).toBeUndefined();
  });

  it("응답 문구와 응답 대기·만료·불필요 상태를 구분한다", () => {
    expect(alertResponseText(response)).toContain("도움이 필요해요");
    expect(alertResponseText({ ...response, answer: "no" })).toContain("괜찮아요");
    expect(alertResponseText({ ...response, status: "PENDING" })).toBe("응답 대기");
    expect(alertResponseText({ ...response, status: "EXPIRED" })).toBe("응답 시간 만료");
    expect(alertResponseText({ ...response, status: "NOT_REQUIRED" })).toBe("응답이 필요하지 않은 알림");
  });
});
