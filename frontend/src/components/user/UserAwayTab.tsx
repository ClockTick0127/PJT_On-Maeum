import AwaySettings from "./AwaySettings";
import type { AwayDraft } from "./AwaySettings";
import UserTabStatus from "./UserTabStatus";
import type { UserTabStatusProps } from "./UserTabStatus";
import Icon from "../common/Icon";

type UserAwayTabProps = {
  draft: AwayDraft;
  busy: boolean;
  dataAvailable: boolean;
  comparison: boolean;
  status: UserTabStatusProps;
  onDraftChange: (draft: AwayDraft) => void;
  onStartAway: (duration: number) => void;
  onBack: () => void;
};

export default function UserAwayTab({
  draft,
  busy,
  dataAvailable,
  comparison,
  status,
  onDraftChange,
  onStartAway,
  onBack,
}: UserAwayTabProps) {
  return (
    <>
      <div>
        <button
          className="flex min-h-14 w-fit items-center justify-center gap-2 rounded-full border border-[#b9c1c9] bg-[linear-gradient(180deg,#ffffff_0%,#f1f3f5_58%,#e3e7eb_100%)] px-6 py-2.5 text-xl leading-snug font-semibold text-stone-900 shadow-[inset_0_1px_0_rgb(255_255_255/90%),0_5px_0_#b1bac3,0_7px_12px_rgb(41_37_36/12%)] transition-[transform,box-shadow,filter] duration-150 hover:brightness-[1.03] active:translate-y-[2px] active:brightness-[.98] active:shadow-[inset_0_1px_0_rgb(255_255_255/70%),0_2px_0_#b1bac3,0_3px_6px_rgb(41_37_36/8%)]"
          type="button"
          onClick={onBack}
        >
          <Icon name="back" />
          이전 화면
        </button>
        {comparison && (
          <p className="mt-2 text-sm font-semibold text-brand-700">기존 디자인 비교 화면</p>
        )}
      </div>
      <UserTabStatus {...status} />
      <AwaySettings
        draft={draft}
        busy={busy}
        dataAvailable={dataAvailable}
        onDraftChange={onDraftChange}
        onStartAway={onStartAway}
      />
    </>
  );
}
