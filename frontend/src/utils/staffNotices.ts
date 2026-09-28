import type { Alert, StatusEvent, Subject } from "../types/monitoring";

export type StaffNoticeKind = "risk" | "help" | "safe" | "expired";

export type StaffNotice = {
  id: string;
  kind: StaffNoticeKind;
  event: StatusEvent;
};

export function isHelpRequested(alert: Alert | null) {
  return alert?.managerStatus !== "RESOLVED"
    && alert?.subjectResponse.status === "ANSWERED"
    && alert.subjectResponse.answer?.toLowerCase() === "yes";
}

export function createStaffNotice(event: StatusEvent, previous?: Subject): StaffNotice | null {
  const response = event.latestAlert?.subjectResponse;
  let kind: StaffNoticeKind;

  if (event.trigger === "SUBJECT_RESPONSE" && response?.status === "ANSWERED") {
    const answer = response.answer?.toLowerCase();
    if (answer !== "yes" && answer !== "no") return null;
    kind = answer === "yes" ? "help" : "safe";
  } else if (event.trigger === "RESPONSE_EXPIRED" && response?.status === "EXPIRED") {
    kind = "expired";
  } else if ((event.trigger === "DETECTION" || event.trigger === "ASSESSMENT") && event.riskLevel === "DANGER") {
    if (
      previous?.riskLevel === "DANGER"
      && event.trigger === "ASSESSMENT"
      && previous.latestAlert?.alertId === event.latestAlert?.alertId
    ) return null;
    kind = "risk";
  } else {
    return null;
  }

  const sourceId = event.latestAlert?.alertId ?? event.lastDetection?.eventId ?? "assessment";
  return { id: `${event.subjectId}:${kind}:${sourceId}`, kind, event };
}

export function addStaffNotice(notices: StaffNotice[], notice: StaffNotice) {
  return [notice, ...notices.filter((current) => current.id !== notice.id)];
}

export type RecentAlertItem = {
  id: string;
  kind: StaffNoticeKind | "check-in";
  subjectId: string;
  score: number;
  time: string;
  source: "stream" | "dashboard";
  targetId: string;
  resolved?: boolean;
};

export function getRecentAlerts(subjects: Subject[], history: StaffNotice[]): RecentAlertItem[] {
  const subjectIds = new Set(subjects.map((subject) => subject.subjectId));
  const items: RecentAlertItem[] = history
    .filter((notice) => subjectIds.has(notice.event.subjectId))
    .map((notice) => ({
      id: notice.id,
      kind: notice.kind,
      subjectId: notice.event.subjectId,
      score: notice.event.riskScore,
      time: notice.event.updatedAt,
      source: "stream",
      targetId: notice.event.latestAlert?.alertId ?? notice.event.lastDetection?.eventId ?? notice.id,
      resolved: notice.event.latestAlert?.managerStatus === "RESOLVED"
        || subjects.some((subject) => subject.latestAlert?.alertId === notice.event.latestAlert?.alertId
          && subject.latestAlert?.managerStatus === "RESOLVED"),
    }));

  for (const subject of subjects) {
    const alert = subject.latestAlert;
    if (!alert) continue;
    const response = alert.subjectResponse;
    const answer = response.answer?.toLowerCase();
    const kind = response.status === "ANSWERED" && answer === "yes" ? "help"
      : response.status === "ANSWERED" && answer === "no" ? "safe"
      : response.status === "EXPIRED" ? "expired" : "check-in";
    if (history.some((notice) => notice.event.subjectId === subject.subjectId
      && notice.event.latestAlert?.alertId === alert.alertId
      && (kind === "check-in" ? notice.kind === "risk" : notice.kind === kind))) continue;
    items.push({
      id: `${subject.subjectId}:${kind}:${alert.alertId}`,
      kind,
      subjectId: subject.subjectId,
      score: subject.riskScore,
      time: response.respondedAt ?? subject.updatedAt,
      source: "dashboard",
      targetId: alert.alertId,
      resolved: alert.managerStatus === "RESOLVED",
    });
  }

  return items.sort((a, b) => Date.parse(b.time) - Date.parse(a.time));
}
