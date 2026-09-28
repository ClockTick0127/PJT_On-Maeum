export type Risk = "NORMAL" | "WARNING" | "DANGER";

export type SubjectResponse = {
  status: string;
  answer: string | null;
  respondedAt: string | null;
};

export type Alert = {
  alertId: string;
  eventId: string | null;
  subjectResponse: SubjectResponse;
  managerStatus?: string;
  managerStatusUpdatedAt?: string | null;
};

export type Subject = {
  subjectId: string;
  name: string;
  age: number;
  address: string;
  phone: string;
  version: number;
  riskLevel: Risk;
  riskScore: number;
  updatedAt: string;
  lastActivity: { occurredAt: string; applianceType: string } | null;
  latestAlert: Alert | null;
  riskTrend: { timezone: string; dailyScores: { date: string; score: number }[] };
};

export type StatusEvent = Pick<
  Subject,
  "subjectId" | "version" | "riskLevel" | "riskScore" | "updatedAt" | "lastActivity" | "latestAlert"
> & {
  trigger: string;
  recentEventCount: number;
  unresolvedAlertCount: number;
  lastDetection: { eventId: string; description: string; occurredAt: string } | null;
};

export type Events = {
  events: {
    eventId: string;
    description: string;
    eventType?: string;
    applianceType?: string | null;
    reason?: Record<string, unknown>;
    riskLevel: Risk;
    riskScore: number;
    occurredAt: string;
    alert: { alertId: string; managerStatus: string; managerStatusUpdatedAt?: string | null; subjectResponse: SubjectResponse } | null;
  }[];
  pagination: { hasNext: boolean; nextCursor: string | null };
};

export type Power = {
  date: string;
  unit: string;
  totalUsage: number;
  updatedAt: string | null;
  hourlyUsage: { hour: number; usage: number | null; status: "COMPLETE" | "PARTIAL" | "NOT_YET" }[];
  appliances: { applianceType: string; totalUsage: number }[];
};

export type MyDashboard = {
  subjectId: string;
  name: string;
  awayMode: {
    enabled: boolean;
    scheduled: boolean;
    startedAt: string | null;
    until: string | null;
  };
  manager: { name: string; phone: string | null } | null;
};

export type SubjectRegistration = {
  name: string;
  birthDate: string;
  phone: string;
  householdId: string;
  address: string;
  addressDetail: string;
  managerMemo: string;
};

export const riskLabels: Record<Risk, string> = { NORMAL: "안정", WARNING: "주의", DANGER: "위험" };

export function responseLabel(response?: SubjectResponse | null) {
  if (!response) return "알림 없음";
  if (response.status === "ANSWERED")
    return response.answer?.toLowerCase() === "yes" ? "도움 요청" : "안전 응답";
  return (
    (
      { PENDING: "응답 대기", EXPIRED: "응답 시간 만료", NOT_REQUIRED: "응답 불필요" } as Record<
        string,
        string
      >
    )[response.status] ?? response.status
  );
}
