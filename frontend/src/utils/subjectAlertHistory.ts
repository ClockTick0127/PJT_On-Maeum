import type { Alert, Events, Risk, Subject, SubjectResponse } from "../types/monitoring";
import type { StaffNotice } from "./staffNotices";

export type SubjectAlertRecord = {
  id: string;
  alertId?: string;
  eventId?: string;
  occurredAt?: string;
  updatedAt: string;
  description?: string;
  riskLevel?: Risk;
  riskScore?: number;
  eventType?: string;
  applianceType?: string | null;
  reason?: Record<string, unknown>;
  managerStatus?: string;
  managerStatusUpdatedAt?: string | null;
  response?: SubjectResponse;
  updates: { id: string; label: string; time: string }[];
  source: "server" | "stream" | "latest";
};

export function alertResponseText(response?: SubjectResponse) {
  if (!response) return "연결된 안부 확인 알림 없음";
  if (response.status === "ANSWERED") {
    if (response.answer?.toLowerCase() === "yes") return "도움 요청 · 도움이 필요해요";
    if (response.answer?.toLowerCase() === "no") return "안전 응답 · 괜찮아요";
    return `응답 완료${response.answer ? ` · ${response.answer}` : ""}`;
  }
  return ({ PENDING: "응답 대기", EXPIRED: "응답 시간 만료", NOT_REQUIRED: "응답이 필요하지 않은 알림" } as Record<string, string>)[response.status] ?? response.status;
}

export function getSubjectAlertHistory(subject: Subject, events: Events["events"], history: StaffNotice[]) {
  const records = new Map<string, SubjectAlertRecord>();
  const applyAlert = (record: SubjectAlertRecord, alert: Alert | NonNullable<Events["events"][number]["alert"]>) => {
    const previousTime = record.response?.respondedAt;
    const nextTime = alert.subjectResponse.respondedAt;
    if (!previousTime || (nextTime && Date.parse(nextTime) >= Date.parse(previousTime))) record.response = alert.subjectResponse;
    if (!record.managerStatusUpdatedAt || (alert.managerStatusUpdatedAt && Date.parse(alert.managerStatusUpdatedAt) >= Date.parse(record.managerStatusUpdatedAt))) {
      record.managerStatus = alert.managerStatus ?? record.managerStatus;
      record.managerStatusUpdatedAt = alert.managerStatusUpdatedAt ?? record.managerStatusUpdatedAt;
    }
    if (alert.subjectResponse.status === "ANSWERED" && nextTime) {
      record.updates.push({ id: `response:${nextTime}:${alert.subjectResponse.answer}`, label: alertResponseText(alert.subjectResponse), time: nextTime });
    }
  };

  for (const event of events) {
    const id = event.alert?.alertId ?? event.eventId;
    const record: SubjectAlertRecord = {
      ...event, id, alertId: event.alert?.alertId, updatedAt: event.occurredAt,
      updates: [{ id: `event:${event.eventId}`, label: "위험 이벤트 기록", time: event.occurredAt }], source: "server",
    };
    if (event.alert) applyAlert(record, event.alert);
    records.set(id, record);
  }

  for (const notice of [...history].sort((a, b) => Date.parse(a.event.updatedAt) - Date.parse(b.event.updatedAt))) {
    const event = notice.event;
    if (event.subjectId !== subject.subjectId) continue;
    const id = event.latestAlert?.alertId ?? event.lastDetection?.eventId ?? notice.id;
    const record: SubjectAlertRecord = records.get(id) ?? {
      id, alertId: event.latestAlert?.alertId, eventId: event.latestAlert?.eventId ?? event.lastDetection?.eventId,
      updatedAt: event.updatedAt, updates: [], source: "stream" as const,
    };
    if (notice.kind === "risk") {
      record.riskLevel ??= event.riskLevel;
      record.riskScore ??= event.riskScore;
      if (event.trigger === "DETECTION" && (!event.latestAlert || event.latestAlert.eventId === event.lastDetection?.eventId)) {
        record.description ??= event.lastDetection?.description;
        record.occurredAt ??= event.lastDetection?.occurredAt;
      }
    }
    const time = notice.kind === "help" || notice.kind === "safe"
      ? event.latestAlert?.subjectResponse.respondedAt ?? event.updatedAt : event.updatedAt;
    if (Date.parse(event.updatedAt) > Date.parse(record.updatedAt)) record.updatedAt = event.updatedAt;
    if (event.latestAlert) applyAlert(record, event.latestAlert);
    const labels = { risk: "위험 신호 감지", help: "도움 요청 · 도움이 필요해요", safe: "안전 응답 · 괜찮아요", expired: "응답 시간 만료" };
    if (!record.updates.some((update) => update.label === labels[notice.kind] && update.time === time)) {
      record.updates.push({ id: notice.id, label: labels[notice.kind], time });
    }
    records.set(id, record);
  }

  if (subject.latestAlert) {
    const alert = subject.latestAlert;
    const record: SubjectAlertRecord = records.get(alert.alertId) ?? {
      id: alert.alertId, alertId: alert.alertId, eventId: alert.eventId ?? undefined,
      updatedAt: alert.subjectResponse.respondedAt ?? subject.updatedAt,
      updates: [], source: "latest" as const,
    };
    applyAlert(record, alert);
    records.set(record.id, record);
  }

  return [...records.values()].map((record) => ({
    ...record,
    updates: record.updates.filter((update, index, list) => list.findIndex((item) => item.label === update.label && item.time === update.time) === index)
      .sort((a, b) => Date.parse(a.time) - Date.parse(b.time)),
    updatedAt: [record.updatedAt, record.response?.respondedAt, record.managerStatusUpdatedAt].filter((time): time is string => Boolean(time))
      .sort((a, b) => Date.parse(b) - Date.parse(a))[0],
  })).sort((a, b) => Date.parse(b.updatedAt) - Date.parse(a.updatedAt));
}
