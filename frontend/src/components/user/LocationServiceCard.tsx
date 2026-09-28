import { useLocationService } from "../../hooks/useLocationService";

export default function LocationServiceCard() {
  const { status, error, enable } = useLocationService();
  const active = status === "active";
  const requesting = status === "requesting";
  const unsupported = status === "unsupported";

  return (
    <section
      className="min-w-0 rounded-3xl border border-stone-200 bg-white p-6 shadow-[0_2px_10px_rgb(15_23_42_/_3%)] max-[359px]:p-5"
      aria-labelledby="location-guide-title"
    >
      <div className="flex items-center gap-4">
        <svg
          className="size-10 shrink-0 text-brand-500"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.7"
          strokeLinecap="round"
          aria-hidden="true"
        >
          <circle cx="12" cy="12" r="7" />
          <circle cx="12" cy="12" r="2.5" />
          <path d="M12 2v3m0 14v3M2 12h3m14 0h3" />
        </svg>
        <div className="min-w-0">
          <h2 id="location-guide-title" className="text-lg font-normal">
            위치 서비스
          </h2>
          <p
            className={`text-2xl font-bold ${active ? "text-emerald-700" : "text-stone-800"}`}
            role="status"
          >
            {active
              ? "켜져 있어요"
              : requesting
                ? "위치 확인 중이에요"
                : unsupported
                  ? "사용할 수 없어요"
                  : "위치를 허용해 주세요"}
          </p>
        </div>
      </div>
      {!active && (
        <>
          <p className="mt-4 text-lg leading-relaxed text-stone-600">
            {unsupported
              ? "위치를 확인할 수 있는 브라우저에서 이용해 주세요."
              : status === "denied"
                ? "브라우저 설정에서 위치를 허용한 뒤 다시 확인해 주세요."
                : status === "error"
                  ? error
                  : "자동 외출 및 재실 기능을 위해 위치서비스를 활용합니다."}
          </p>
          {!unsupported && (
            <button
              className="mt-4 flex min-h-16 w-full items-center justify-center rounded-full border border-[#b9c1c9] bg-[linear-gradient(180deg,#ffffff_0%,#f1f3f5_58%,#e3e7eb_100%)] px-7 py-3.5 text-center text-[1.3125rem] leading-6 font-semibold text-stone-800 shadow-[inset_0_1px_0_rgb(255_255_255/90%),0_5px_0_#b1bac3,0_9px_18px_rgb(15_23_42/10%)] transition-[transform,box-shadow,filter] duration-150 hover:brightness-[.99] active:translate-y-[3px] active:shadow-[0_2px_0_#b1bac3,0_4px_8px_rgb(15_23_42/8%)] disabled:cursor-not-allowed disabled:opacity-60"
              type="button"
              disabled={requesting}
              onClick={enable}
            >
              {requesting
                ? "확인 중…"
                : status === "denied" || status === "error"
                  ? "다시 확인하기"
                  : "위치 허용하기"}
            </button>
          )}
        </>
      )}
    </section>
  );
}
