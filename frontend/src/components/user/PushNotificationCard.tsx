import type { PushSubscriptionStatus } from "../../services/pushSubscription";
import alertIcon from "../../assets/user/alert.svg";

type PushNotificationCardProps = {
  status: PushSubscriptionStatus;
  error: string;
  onEnable: () => void;
  onRetry: () => void;
};

export default function PushNotificationCard({
  status,
  error,
  onEnable,
  onRetry,
}: PushNotificationCardProps) {
  if (status === "unsupported" || status === "unconfigured") return null;

  if (status === "subscribed") {
    return (
      <section
        className="flex gap-4 items-center min-w-0 rounded-3xl border border-stone-200 bg-white p-6 shadow-[0_2px_10px_rgb(15_23_42_/_3%)] max-[359px]:p-5"
        role="status"
      >
        <div className="size-10">
          <img src={alertIcon} />
        </div>
        <div>
          <p className="text-lg">안심 알림</p>
          <p className="text-2xl font-bold text-emerald-700">켜져 있어요</p>
        </div>
      </section>
    );
  }

  const denied = status === "denied";
  return (
    <section
      className="min-w-0 rounded-3xl border border-stone-200 bg-white p-6 shadow-[0_2px_10px_rgb(15_23_42_/_3%)] max-[359px]:p-5"
      aria-labelledby="push-title"
    >
      <p className="text-lg">안심 알림</p>
      <h2 className="text-2xl font-bold" id="push-title">
        알림을 켜 주세요
      </h2>
      <p className="mt-2 text-stone-600">
        {denied
          ? "브라우저 설정에서 이 서비스의 알림을 허용한 뒤 다시 확인해 주세요."
          : "위험 신호가 확인되면 바로 알려드릴 수 있도록 알림을 켜 주세요."}
      </p>
      {status === "error" && (
        <p className="mt-3 rounded-xl bg-red-50 p-3 text-red-700" role="alert">
          {error}
        </p>
      )}
      <button
        className="mt-4 flex min-h-16 w-full items-center justify-center rounded-full border border-[#b9c1c9] bg-[linear-gradient(180deg,#ffffff_0%,#f1f3f5_58%,#e3e7eb_100%)] px-7 py-3.5 text-center text-[1.3125rem] leading-6 font-semibold text-stone-800 shadow-[inset_0_1px_0_rgb(255_255_255_/_90%),0_5px_0_#b1bac3,0_9px_18px_rgb(15_23_42_/_10%)] transition-[transform,box-shadow,filter] duration-150 hover:brightness-[.99] active:translate-y-[3px] active:shadow-[inset_0_1px_0_rgb(255_255_255_/_70%),0_2px_0_#b1bac3,0_4px_8px_rgb(15_23_42_/_8%)] disabled:cursor-not-allowed disabled:border-stone-200 disabled:bg-stone-200 disabled:text-stone-600 disabled:shadow-none"
        disabled={status === "syncing"}
        onClick={status === "error" || denied ? onRetry : onEnable}
      >
        {status === "syncing" ? "알림 설정 확인 중…" : denied ? "다시 확인하기" : "알림 켜기"}
      </button>
    </section>
  );
}
