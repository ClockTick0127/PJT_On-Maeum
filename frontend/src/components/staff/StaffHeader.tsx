type StaffHeaderProps = {
  title?: string;
  connection: "connecting" | "connected" | "disconnected";
  refreshing: boolean;
  onRefresh: () => void;
};

export default function StaffHeader({
  title,
  connection,
  refreshing,
  onRefresh,
}: StaffHeaderProps) {
  return (
    <header className="rounded-t-[1.75rem] lg:rounded-t-[2rem] flex min-h-20 items-center justify-between border-b border-stone-200/60 bg-[#f5f5f4] px-5 md:px-10">
      <span className="text-base font-medium text-stone-600">{title}</span>
      <div className="flex items-center gap-3">
        <span className="hidden text-sm text-stone-500 sm:block" role="status">
          {connection === "connected"
            ? "연결됨"
            : connection === "connecting"
              ? "연결 중"
              : "재연결 중"}
        </span>
        <button
          className="rounded-lg border border-stone-200 bg-white px-4 py-2 text-base font-semibold text-stone-600 transition-colors hover:border-brand-200 hover:text-brand-700 disabled:opacity-50"
          disabled={refreshing}
          onClick={onRefresh}
        >
          {refreshing ? "갱신 중…" : "새로고침"}
        </button>
      </div>
    </header>
  );
}
