import { useMemo, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { resolveNotification, type SubjectsResponse } from "../api/monitoring";
import { monitoringKeys } from "../hooks/api/monitoringKeys";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { getApiErrorMessage } from "../api/client";
import { clearSession } from "../api/tokenStorage";
import Icon from "../components/common/Icon";
import OverviewMetrics from "../components/staff/OverviewMetrics";
import RecentAlerts from "../components/staff/RecentAlerts";
import RegistrationForm from "../components/staff/RegistrationForm";
import StaffHeader from "../components/staff/StaffHeader";
import StaffPageHeading from "../components/staff/StaffPageHeading";
import StaffSidebar from "../components/staff/StaffSidebar";
import { staffNavigation } from "../components/staff/staffNavigation";
import type { StaffView } from "../components/staff/staffNavigation";
import StaffNotifications from "../components/staff/StaffNotifications";
import SubjectDetail from "../components/staff/SubjectDetail";
import SubjectList from "../components/staff/SubjectList";
import { useMeQuery, useSubjectsQuery } from "../hooks/api";
import { useSubjectStream } from "../hooks/realtime/useSubjectStream";
import { unsubscribeFromPush } from "../services/pushSubscription";
import { applyStaffDemoSubjects, LIVE_SUBJECT_ID } from "../utils/staffDemo";

export default function StaffPage() {
  const navigate = useNavigate();
  const { subjectId } = useParams();
  const [searchParams] = useSearchParams();
  const query = useSubjectsQuery();
  const meQuery = useMeQuery();
  const stream = useSubjectStream();
  const queryClient = useQueryClient();
  const resolveMutation = useMutation({
    mutationFn: resolveNotification,
    onSuccess: (_data, alertId) => {
      stream.resolveAlert(alertId);
      queryClient.setQueryData<SubjectsResponse>(monitoringKeys.subjects, (current) => current && ({
        ...current,
        subjects: current.subjects.map((subject) => subject.latestAlert?.alertId === alertId
          ? { ...subject, latestAlert: { ...subject.latestAlert, managerStatus: "RESOLVED", managerStatusUpdatedAt: new Date().toISOString() } }
          : subject),
      }));
      void queryClient.invalidateQueries({ queryKey: monitoringKeys.subjects });
    },
  });
  const [registering, setRegistering] = useState(false);
  const [feedback, setFeedback] = useState("");
  const subjects = useMemo(() => applyStaffDemoSubjects(query.data?.subjects ?? []), [query.data?.subjects]);
  const notices = stream.notices.filter((notice) => notice.event.subjectId === LIVE_SUBJECT_ID
    && notice.event.latestAlert?.managerStatus !== "RESOLVED"
    && !subjects.some((subject) => subject.latestAlert?.alertId === notice.event.latestAlert?.alertId
      && subject.latestAlert?.managerStatus === "RESOLVED"));
  const history = stream.history.filter((notice) => notice.event.subjectId === LIVE_SUBJECT_ID);
  const requestedView = searchParams.get("view");
  const view: StaffView = staffNavigation.some((item) => item.key === requestedView)
    ? (requestedView as StaffView)
    : "overview";
  const selected = subjects.find((subject) => subject.subjectId === subjectId);
  const showNotifications = !subjectId && view === "overview";
  const title = subjectId
    ? "대상자 상세"
    : staffNavigation.find((item) => item.key === view)?.label;

  const logout = async () => {
    await Promise.allSettled([unsubscribeFromPush()]);
    clearSession();
    navigate("/");
  };

  return (
    <main className="min-h-screen bg-[#fcfcfb] text-stone-800 lg:grid lg:grid-cols-[250px_1fr]">
      <StaffSidebar
        view={view}
        subjectId={subjectId}
        manager={
          meQuery.data
            ? {
                name: meQuery.data.profile.displayName,
                email: meQuery.data.profile.email,
              }
            : undefined
        }
        onLogout={logout}
        profileError={meQuery.isError}
        onRetryProfile={() => void meQuery.refetch()}
      />
      <div className="m-3 ml-0 min-w-0 rounded-[1.75rem] border border-stone-200/80 bg-[#f5f5f4] lg:col-start-2 lg:m-6 lg:ml-0 lg:rounded-[2rem]">
        <StaffHeader
          title={title}
          connection={stream.connection}
          refreshing={query.isFetching}
          onRefresh={() => void query.refetch()}
        />
        <div className={`mx-auto grid max-w-[1440px] gap-6 p-5 md:p-8 xl:p-10 ${showNotifications && notices.length ? "pb-32 md:pb-32 xl:pb-32" : ""}`}>
          {query.isError && (
            <p className="rounded-xl bg-red-50 p-4 text-red-700" role="alert">
              {getApiErrorMessage(query.error)}
            </p>
          )}
          {resolveMutation.isError && (
            <p className="rounded-xl bg-red-50 p-4 text-red-700" role="alert">
              해결 처리에 실패했습니다. {getApiErrorMessage(resolveMutation.error)} 다시 시도해 주세요.
            </p>
          )}
          {stream.connection === "disconnected" && (
            <p className="rounded-xl bg-amber-50 p-4 text-amber-800" role="status">
              실시간 연결이 끊겨 재연결 중입니다. {stream.error}
            </p>
          )}
          {showNotifications && <StaffNotifications notices={notices} subjects={subjects} onDismiss={stream.dismissNotice} />}
          {subjectId ? (
            <>
              <Link
                className="flex w-fit items-center gap-2 font-semibold text-brand-700"
                to="/staff?view=subjects"
              >
                <Icon name="back" />
                대상자 목록
              </Link>
              {selected ? (
                <SubjectDetail subject={selected} history={history} demo={selected.subjectId !== LIVE_SUBJECT_ID} />
              ) : (
                <section className="rounded-2xl bg-white p-10 text-center text-stone-500">
                  {query.isLoading
                    ? "대상자 정보를 불러오는 중입니다."
                    : "담당 대상자를 찾을 수 없습니다."}
                </section>
              )}
            </>
          ) : (
            <>
              <StaffPageHeading
                view={view}
                title={title}
                onToggleRegistration={() => setRegistering((value) => !value)}
              />
              {registering && (
                <RegistrationForm
                  onCancel={() => setRegistering(false)}
                  onDone={() => {
                    setRegistering(false);
                    setFeedback("대상자를 등록했습니다.");
                  }}
                />
              )}
              {view === "overview" && (
                <>
                  <OverviewMetrics subjects={subjects} loading={query.isLoading} />
                </>
              )}
              {view === "alerts" ? (
                <RecentAlerts subjects={subjects} history={history} loading={query.isLoading} />
              ) : (
                <SubjectList
                  subjects={subjects}
                  loading={query.isLoading}
                  dataUpdatedAt={query.dataUpdatedAt}
                  onResolve={(alertId) => resolveMutation.mutate(alertId)}
                  resolving={resolveMutation.isPending}
                />
              )}
            </>
          )}
          {feedback && (
            <p
              className={`fixed right-5 rounded-xl bg-stone-800 px-5 py-3 text-white shadow-lg ${showNotifications && notices.length ? "bottom-28" : "bottom-5"}`}
              role="status"
            >
              {feedback}
            </p>
          )}
        </div>
      </div>
    </main>
  );
}
