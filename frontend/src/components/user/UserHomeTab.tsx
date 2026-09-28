import LocationServiceCard from "./LocationServiceCard";
import type { MyDashboard } from "../../types/monitoring";
import PushNotificationCard from "./PushNotificationCard";
import UserDashboard from "./UserDashboard";
import UserTabStatus from "./UserTabStatus";
import type { UserTabStatusProps } from "./UserTabStatus";

type UserHomeTabProps = {
  data?: MyDashboard;
  comparison: boolean;
  busy: boolean;
  status: UserTabStatusProps;
  onUpdateAway: (enabled: boolean) => void;
};

export default function UserHomeTab({
  data,
  comparison,
  busy,
  status,
  onUpdateAway,
}: UserHomeTabProps) {
  return (
    <>
      <div>
        {data ? (
          <div className="text-center">
            <p className="text-base font-bold">안녕하세요</p>
            <p className="text-3xl leading-snug font-extrabold">{data.name}님</p>
          </div>
        ) : (
          <p className="text-center text-4xl leading-snug font-bold">안녕하세요</p>
        )}
        {comparison && (
          <p className="mt-2 text-sm font-semibold text-brand-700">기존 디자인 비교 화면</p>
        )}
      </div>
      <UserTabStatus {...status} />
      <UserDashboard
        data={data}
        busy={busy}
        onUpdateAway={onUpdateAway}
      />
      <LocationServiceCard />
      <PushNotificationCard
        status={status.pushStatus}
        error={status.pushError}
        onEnable={status.onEnablePush}
        onRetry={status.onRetryPush}
      />
    </>
  );
}
