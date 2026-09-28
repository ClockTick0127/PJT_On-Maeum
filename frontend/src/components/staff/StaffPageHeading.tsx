import type { StaffView } from "./staffNavigation";

type StaffPageHeadingProps = {
  view: StaffView;
  title?: string;
  onToggleRegistration: () => void;
};

export default function StaffPageHeading({
  view,
  title,
  onToggleRegistration,
}: StaffPageHeadingProps) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-4">
      <h1 className="text-2xl font-bold tracking-tight md:text-[1.875rem]">
        {view === "overview" ? "대상자 현황" : title}
      </h1>
      {view !== "alerts" && (
        <button
          className="flex items-center gap-2 rounded-lg bg-brand-500 px-5 py-3 text-base font-semibold text-white transition-colors hover:bg-brand-600"
          onClick={onToggleRegistration}
        >
          대상자 등록
        </button>
      )}
    </div>
  );
}
