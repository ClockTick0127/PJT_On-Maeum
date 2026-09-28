import type { MyDashboard } from "../../types/monitoring";
import { request } from "../client";

export type AwayModeRequest = {
  enabled: boolean;
  endsAt?: string;
};

export type NotificationAnswer = "yes" | "no";

export const getMyDashboard = () => request<MyDashboard>({ url: "/monitoring/my-dashboard" });

export const updateAwayMode = (data: AwayModeRequest) =>
  request<void>({
    url: "/monitoring/my-dashboard/away-mode",
    method: "PUT",
    data,
  });

export const answerNotification = (notificationId: string, answer: NotificationAnswer) =>
  request<void>({
    url: `/monitoring/notifications/${notificationId}/responses`,
    method: "PUT",
    data: { answer, source: "user", respondedAt: new Date().toISOString() },
  });
