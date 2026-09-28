import { describe, expect, it } from "vitest";
import type { StatusEvent, Subject } from "../src/types/monitoring";
import { addStaffNotice, createStaffNotice, getRecentAlerts, isHelpRequested } from "../src/utils/staffNotices";

const event: StatusEvent = {
  subjectId: "1",
  version: 2,
  trigger: "DETECTION",
  riskLevel: "DANGER",
  riskScore: 96,
  updatedAt: "2026-09-26T10:00:00Z",
  lastActivity: null,
  latestAlert: {
    alertId: "alert-1",
    eventId: "event-1",
    subjectResponse: { status: "PENDING", answer: null, respondedAt: null },
  },
  recentEventCount: 1,
  unresolvedAlertCount: 1,
  lastDetection: null,
};

function responseEvent(answer: string | null, status = "ANSWERED"): StatusEvent {
  return {
    ...event,
    version: 3,
    trigger: "SUBJECT_RESPONSE",
    latestAlert: {
      alertId: "alert-1",
      eventId: "event-1",
      subjectResponse: { status, answer, respondedAt: event.updatedAt },
    },
  };
}

describe("담당자 알림 분류", () => {
  it("위험 감지와 도움 요청을 같은 알림 번호여도 별도로 유지한다", () => {
    const risk = createStaffNotice(event)!;
    const help = createStaffNotice(responseEvent("YES"))!;
    expect(risk.kind).toBe("risk");
    expect(help.kind).toBe("help");
    const notices = addStaffNotice(addStaffNotice([], risk), help);
    expect(notices.map((notice) => notice.kind)).toEqual(["help", "risk"]);
    expect(addStaffNotice(notices, help)).toHaveLength(2);
  });

  it.each(["yes", "YES"])("%s 응답은 낮은 점수여도 도움 요청으로 표시한다", (answer) => {
    expect(createStaffNotice({ ...responseEvent(answer), riskLevel: "NORMAL", riskScore: 10 })?.kind).toBe("help");
  });

  it.each(["no", "NO"])("%s 응답은 위험 점수가 높아도 안전 응답으로 표시한다", (answer) => {
    expect(createStaffNotice(responseEvent(answer))?.kind).toBe("safe");
  });

  it("기존 도움 요청 값이 남아 있어도 새 위험 감지를 도움 요청으로 바꾸지 않는다", () => {
    expect(createStaffNotice({ ...responseEvent("YES"), trigger: "DETECTION" })?.kind).toBe("risk");
  });

  it.each(["ACTIVITY", "AWAY_MODE", "MANAGER_STATUS", "PROFILE_UPDATED"])("위험 상태의 %s 갱신은 새 위험 알림으로 표시하지 않는다", (trigger) => {
    expect(createStaffNotice({ ...event, trigger })).toBeNull();
  });

  it("유효한 응답이 없는 대상자 갱신은 응답 알림으로 만들지 않는다", () => {
    expect(createStaffNotice(responseEvent(null, "PENDING"))).toBeNull();
    expect(createStaffNotice(responseEvent("unknown"))).toBeNull();
  });

  it("응답 시간 만료는 도움 요청과 구분한다", () => {
    expect(createStaffNotice({ ...responseEvent(null, "EXPIRED"), trigger: "RESPONSE_EXPIRED" })?.kind).toBe("expired");
  });

  it("같은 위험 상태의 자체 평가 반복은 생략하고 새 알림은 표시한다", () => {
    const previous: Subject = {
      ...event,
      name: "박정수",
      age: 74,
      address: "가상 주소",
      phone: "",
      riskTrend: { timezone: "Asia/Seoul", dailyScores: [] },
    };
    expect(createStaffNotice({ ...event, trigger: "ASSESSMENT" }, previous)).toBeNull();
    expect(createStaffNotice({ ...event, trigger: "ASSESSMENT" }, { ...previous, riskLevel: "WARNING" })?.kind).toBe("risk");
    expect(createStaffNotice({ ...event, trigger: "ASSESSMENT" }, { ...previous, latestAlert: null })?.kind).toBe("risk");
  });

  it("조치 완료 또는 안전 응답에는 도움 요청 강조를 하지 않는다", () => {
    const alert = responseEvent("YES").latestAlert!;
    expect(isHelpRequested(alert)).toBe(true);
    expect(isHelpRequested({ ...alert, managerStatus: "RESOLVED" })).toBe(false);
    expect(isHelpRequested(responseEvent("NO").latestAlert)).toBe(false);
    expect(isHelpRequested(null)).toBe(false);
  });
});

describe("최근 알림 목록", () => {
  const subject: Subject = {
    ...event,
    name: "박정수",
    age: 74,
    address: "가상 주소",
    phone: "",
    riskTrend: { timezone: "Asia/Seoul", dailyScores: [] },
  };

  it("같은 대상자의 위험 감지와 도움 요청을 별도 행으로 표시한다", () => {
    const helpEvent = responseEvent("YES");
    const history = [createStaffNotice(helpEvent)!, createStaffNotice(event)!];
    const rows = getRecentAlerts([{ ...subject, latestAlert: helpEvent.latestAlert }], history);
    expect(rows).toHaveLength(2);
    expect(rows.map((row) => row.kind)).toEqual(["help", "risk"]);
  });

  it("실시간 기록이 없어도 서버 최신 알림을 표시한다", () => {
    const rows = getRecentAlerts([subject], []);
    expect(rows).toHaveLength(1);
    expect(rows[0].kind).toBe("check-in");
    expect(rows[0].source).toBe("dashboard");
  });

  it("서버 최신 도움 요청과 실시간 위험 기록을 함께 표시한다", () => {
    const rows = getRecentAlerts([{ ...subject, latestAlert: responseEvent("YES").latestAlert }], [createStaffNotice(event)!]);
    expect(rows).toHaveLength(2);
    expect(rows.some((row) => row.kind === "help" && row.source === "dashboard")).toBe(true);
  });

  it("동일한 서버 알림은 중복시키지 않고 담당 대상자의 기록만 표시한다", () => {
    const risk = createStaffNotice(event)!;
    expect(getRecentAlerts([subject], [risk])).toHaveLength(1);
    expect(getRecentAlerts([], [risk])).toEqual([]);
  });

  it("점수 순서가 아닌 발생 시각의 최신순으로 표시한다", () => {
    const risk = createStaffNotice(event)!;
    const help = createStaffNotice({ ...responseEvent("YES"), riskScore: 10, updatedAt: "2026-09-26T10:01:00Z" })!;
    expect(getRecentAlerts([subject], [risk, help])[0].kind).toBe("help");
  });
});
