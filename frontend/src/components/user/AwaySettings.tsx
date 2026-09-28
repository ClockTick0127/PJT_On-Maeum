export type AwayDraft = {
  duration: number;
};

type AwaySettingsProps = {
  draft: AwayDraft;
  busy: boolean;
  dataAvailable: boolean;
  onDraftChange: (draft: AwayDraft) => void;
  onStartAway: (duration: number) => void;
};

export default function AwaySettings({
  draft,
  busy,
  dataAvailable,
  onDraftChange,
  onStartAway,
}: AwaySettingsProps) {
  return (
    <>
      <fieldset disabled={busy || !dataAvailable} aria-label="외출 시간 선택">
        <div className="grid grid-cols-1 gap-4">
          {[30, 60, 120, 240].map((minutes) => (
            <button
              type="button"
              className="text-[32px] font-semibold flex min-h-16 w-full items-center justify-center rounded-full border border-[#e67830] bg-[linear-gradient(180deg,#f58b40_0%,#f37929_58%,#e96e24_100%)] px-7 py-3.5 text-center text-white shadow-[inset_0_1px_0_rgb(255_255_255/32%),0_5px_0_#c96524,0_9px_18px_rgb(120_63_27/18%)] transition-[transform,box-shadow,filter] duration-150 hover:not-disabled:brightness-[1.02] active:not-disabled:translate-y-[3px] active:not-disabled:brightness-[.97] active:not-disabled:shadow-[inset_0_1px_0_rgb(255_255_255_/_18%),0_2px_0_#c96524,0_4px_8px_rgb(120_63_27_/_12%)] disabled:cursor-not-allowed disabled:border-stone-300 disabled:bg-none disabled:bg-stone-300 disabled:text-stone-600 disabled:shadow-none"
              key={minutes}
              onClick={() => {
                onDraftChange({ ...draft, duration: minutes });
                onStartAway(minutes);
              }}
            >
              {busy && draft.duration === minutes
                ? "처리 중…"
                : minutes < 60
                  ? `${minutes}분 외출 하기`
                  : `${minutes / 60}시간 외출 하기`}
            </button>
          ))}
        </div>
      </fieldset>
      <div className="mt-6 grid gap-4 border-t border-stone-200 pt-5">
        <details className="group">
          <summary className="min-h-16 cursor-pointer rounded-2xl border border-stone-300 p-4 font-semibold text-stone-800 transition-colors hover:bg-stone-50">
            외출 시간 직접 입력하기
          </summary>
          <div className="mt-4 grid gap-3">
            <label className="grid gap-2 font-semibold">
              외출 시간 (분)
              <input
                className="h-auto min-h-15 w-full min-w-0 rounded-xl border border-stone-400 bg-white p-3 text-stone-800"
                type="number"
                inputMode="numeric"
                min="1"
                max="1440"
                step="1"
                value={draft.duration || ""}
                disabled={busy}
                onChange={(event) =>
                  onDraftChange({ ...draft, duration: Number(event.target.value) })
                }
              />
            </label>
            <button
              className="flex min-h-16 w-full items-center justify-center rounded-full border border-[#e67830] bg-[linear-gradient(180deg,#f58b40_0%,#f37929_58%,#e96e24_100%)] px-5 py-3.5 text-xl font-bold text-white shadow-[0_5px_0_#c96524,0_7px_14px_rgb(243_121_41_/_20%)] transition-[transform,box-shadow,filter] duration-150 hover:not-disabled:brightness-[1.02] active:not-disabled:translate-y-[3px] active:not-disabled:shadow-[0_2px_0_#c96524,0_3px_6px_rgb(74_54_35_/_8%)] disabled:cursor-not-allowed disabled:bg-none disabled:bg-stone-300 disabled:text-stone-600 disabled:shadow-none"
              type="button"
              disabled={busy || !dataAvailable}
              onClick={() => onStartAway(draft.duration)}
            >
              {busy ? "처리 중…" : "입력한 시간으로 외출하기"}
            </button>
          </div>
        </details>
      </div>
    </>
  );
}
