import type { IconName } from "../common/Icon";

export type StaffView = "overview" | "subjects" | "alerts";

export const staffNavigation: { key: StaffView; label: string; icon: IconName }[] = [
  { key: "overview", label: "대시보드", icon: "grid" },
  { key: "subjects", label: "대상자 관리", icon: "users" },
  { key: "alerts", label: "최근 알림", icon: "bell" },
];
