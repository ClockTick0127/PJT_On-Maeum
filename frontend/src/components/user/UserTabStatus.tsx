import type { PushSubscriptionStatus } from "../../services/pushSubscription";

export type UserTabStatusProps = {
  dashboardError: string;
  pushStatus: PushSubscriptionStatus;
  pushError: string;
  onRetryDashboard: () => void;
  onEnablePush: () => void;
  onRetryPush: () => void;
};

export default function UserTabStatus({
  dashboardError,
  onRetryDashboard,
}: UserTabStatusProps) {
  return (
    <>
      {dashboardError && (
        <div className="rounded-xl bg-red-50 p-4 text-red-700" role="alert">
          <p>{dashboardError}</p>
          <button className="mt-2 font-bold underline" onClick={onRetryDashboard}>
            다시 불러오기
          </button>
        </div>
      )}
    </>
  );
}
