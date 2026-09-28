import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  answerNotification,
  registerSubject,
  updateAwayMode,
  type NotificationAnswer,
} from "../../api/monitoring";
import type { SubjectRegistration } from "../../types/monitoring";
import { monitoringKeys } from "./monitoringKeys";

type NotificationResponseVariables = {
  notificationId: string;
  answer: NotificationAnswer;
};

export function useRegisterSubjectMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (subject: SubjectRegistration) => registerSubject(subject),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: monitoringKeys.subjects }),
  });
}

export function useAwayModeMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: updateAwayMode,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: monitoringKeys.myDashboard }),
  });
}

export function useNotificationResponseMutation() {
  return useMutation({
    mutationFn: ({ notificationId, answer }: NotificationResponseVariables) =>
      answerNotification(notificationId, answer),
  });
}
