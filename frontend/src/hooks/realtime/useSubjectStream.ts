import { useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { getApiErrorMessage } from "../../api/client";
import type { SubjectsResponse } from "../../api/monitoring";
import { readSubjectStream } from "../../api/stream";
import type { StatusEvent } from "../../types/monitoring";
import { monitoringKeys } from "../api/monitoringKeys";
import { addStaffNotice, createStaffNotice } from "../../utils/staffNotices";
import type { StaffNotice } from "../../utils/staffNotices";

export function useSubjectStream() {
  const queryClient = useQueryClient();
  const [connection, setConnection] = useState<"connecting" | "connected" | "disconnected">(
    "connecting",
  );
  const [error, setError] = useState("");
  const [notices, setNotices] = useState<StaffNotice[]>([]);
  const [history, setHistory] = useState<StaffNotice[]>([]);

  useEffect(() => {
    let active = true;
    let retries = 0;
    let timer: number | undefined;
    let controller: AbortController | undefined;
    const receivedVersions = new Map<string, number>();

    const connect = async () => {
      controller = new AbortController();

      try {
        await readSubjectStream(
          controller.signal,
          () => {
            if (!active) return;
            retries = 0;
            setConnection("connected");
            setError("");
            queryClient.invalidateQueries({ queryKey: monitoringKeys.subjects }).catch((cause) => {
              if (active) setError(getApiErrorMessage(cause));
            });
          },
          (eventName, payload) => {
            if (!active || eventName !== "subject-status") return;
            const event = payload as StatusEvent;
            const previous = queryClient.getQueryData<SubjectsResponse>(monitoringKeys.subjects)
              ?.subjects.find((subject) => subject.subjectId === event.subjectId);
            if (event.version <= (receivedVersions.get(event.subjectId) ?? -1)
              || (previous && event.version < previous.version)) return;
            receivedVersions.set(event.subjectId, event.version);
            const resolvedAlert = event.latestAlert;
            if (resolvedAlert?.managerStatus === "RESOLVED") {
              setNotices((current) => current.filter((notice) => notice.event.latestAlert?.alertId !== resolvedAlert.alertId));
              setHistory((current) => current.map((notice) => notice.event.latestAlert?.alertId === resolvedAlert.alertId
                ? { ...notice, event: { ...notice.event, latestAlert: resolvedAlert } }
                : notice));
            }
            queryClient.setQueryData<SubjectsResponse>(monitoringKeys.subjects, (current) =>
              current
                ? {
                    subjects: current.subjects.map((subject) =>
                      subject.subjectId === event.subjectId && event.version > subject.version
                        ? { ...subject, ...event }
                        : subject,
                    ),
                  }
                : current,
            );
            const notice = createStaffNotice(event, previous);
            if (notice) {
              setNotices((current) => addStaffNotice(current, notice));
              setHistory((current) => addStaffNotice(current, notice));
            }
          },
        );
      } catch (cause) {
        if (active) setError(getApiErrorMessage(cause));
      }

      if (active) {
        setConnection("disconnected");
        timer = window.setTimeout(
          () => connect().catch(() => undefined),
          Math.min(30_000, 1_000 * 2 ** Math.min(retries++, 5)),
        );
      }
    };

    connect().catch(() => undefined);

    return () => {
      active = false;
      controller?.abort();
      window.clearTimeout(timer);
    };
  }, [queryClient]);

  const dismissNotice = (id: string) => {
    setNotices((current) => current.filter((notice) => notice.id !== id));
  };

  const resolveAlert = (alertId: string) => {
    setNotices((current) => current.filter((notice) => notice.event.latestAlert?.alertId !== alertId));
    setHistory((current) => current.map((notice) => notice.event.latestAlert?.alertId === alertId
      ? { ...notice, event: { ...notice.event, latestAlert: { ...notice.event.latestAlert, managerStatus: "RESOLVED", managerStatusUpdatedAt: new Date().toISOString() } } }
      : notice));
  };

  return { connection, error, notices, history, dismissNotice, resolveAlert };
}
