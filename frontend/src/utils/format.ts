import type { Risk } from "../types/monitoring";

export function formatDateTime(value?: string | null) {
  return value && Number.isFinite(Date.parse(value))
    ? new Date(value).toLocaleString("ko-KR", { timeZone: "Asia/Seoul", hour12: false })
    : "기록 없음";
}

export function formatShortDateTime(value?: string | null) {
  return value
    ? new Date(value).toLocaleString("ko-KR", {
        month: "long",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
      })
    : "종료 시간 미정";
}

export function todayInSeoul() {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Seoul",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
}

export function riskBadgeClass(risk: Risk) {
  if (risk === "DANGER") return "bg-red-100 text-red-700 ring-red-200";
  if (risk === "WARNING") return "bg-amber-100 text-amber-700 ring-amber-200";
  return "bg-emerald-100 text-emerald-700 ring-emerald-200";
}

export function telephoneHref(phone: string) {
  return `tel:${phone.replace(/[^+\d]/g, "")}`;
}
