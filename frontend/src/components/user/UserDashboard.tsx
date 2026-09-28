import phoneImage from "../../assets/user/phone.svg";
import walkImage from "../../assets/user/walk.svg";
import type { MyDashboard } from "../../types/monitoring";
import { telephoneHref } from "../../utils/format";
import managerIcon from "../../assets/user/manager.svg";

type UserDashboardProps = {
  data?: MyDashboard;
  busy: boolean;
  onUpdateAway: (enabled: boolean) => void;
};

export default function UserDashboard({
  data,
  busy,
  onUpdateAway,
}: UserDashboardProps) {
  const mode = data?.awayMode;
  const hasAwayMode = Boolean(mode?.enabled);

  return (
    <>
      {hasAwayMode ? (
        <button
          className="text-[32px] font-semibold flex min-h-16 w-full items-center justify-center gap-2 rounded-full border border-[#e67830] bg-[linear-gradient(180deg,#f58b40_0%,#f37929_58%,#e96e24_100%)] px-7 py-3.5 text-center text-white shadow-[inset_0_1px_0_rgb(255_255_255/32%),0_5px_0_#c96524,0_9px_18px_rgb(120_63_27/18%)] transition-[transform,box-shadow,filter] duration-150 hover:not-disabled:brightness-[1.02] active:not-disabled:translate-y-[3px] active:not-disabled:brightness-[.97] active:not-disabled:shadow-[inset_0_1px_0_rgb(255_255_255_/_18%),0_2px_0_#c96524,0_4px_8px_rgb(120_63_27_/_12%)] disabled:cursor-not-allowed disabled:border-[#c75a17] disabled:text-white disabled:brightness-[.9] disabled:shadow-none"
          disabled={busy}
          onClick={() => onUpdateAway(false)}
        >
          {/* <img className="size-8" src={homeImage} alt="" aria-hidden="true" /> */}
          {busy ? "처리 중…" : "집에 돌아왔어요"}
        </button>
      ) : (
        <button
          className="text-[32px] font-semibold flex min-h-16 w-full items-center justify-center gap-2 rounded-full border border-[#e67830] bg-[linear-gradient(180deg,#f58b40_0%,#f37929_58%,#e96e24_100%)] px-7 py-3.5 text-center text-white shadow-[inset_0_1px_0_rgb(255_255_255/32%),0_5px_0_#c96524,0_9px_18px_rgb(120_63_27/18%)] transition-[transform,box-shadow,filter] duration-150 hover:not-disabled:brightness-[1.02] active:not-disabled:translate-y-[3px] active:not-disabled:brightness-[.97] active:not-disabled:shadow-[inset_0_1px_0_rgb(255_255_255_/_18%),0_2px_0_#c96524,0_4px_8px_rgb(120_63_27_/_12%)] disabled:cursor-not-allowed disabled:border-[#c75a17] disabled:text-white disabled:brightness-[.9] disabled:shadow-none"
          disabled={!data || busy}
          data-user-action="away"
          onClick={() => onUpdateAway(true)}
        >
          <div className="size-9">
            <img src={walkImage} alt="" aria-hidden="true" />
          </div>
          <span>{busy ? "처리 중…" : "외출하기"}</span>
        </button>
      )}
      <section
        className="min-w-0 rounded-3xl border border-stone-200 bg-white p-6 shadow-[0_2px_10px_rgb(15_23_42_/_3%)] max-[359px]:p-5"
        aria-labelledby="contact-title"
      >
        {data?.manager ? (
          <>
            <div className="flex gap-4 items-center">
              <div className="size-12">
                <img src={managerIcon} />
              </div>
              <div>
                <p className="text-lg">담당자</p>
                <p className="text-2xl font-bold">{data.manager.name}</p>
              </div>
            </div>
            {data.manager.phone ? (
              <>
                {/* <p className="my-2 text-lg text-stone-600">{data.manager.phone}</p> */}
                <a
                  className="flex min-h-16 w-full text-[24px] font-semibold items-center justify-center mt-4 gap-2.5 rounded-full border border-[#b9c1c9] bg-[linear-gradient(180deg,#ffffff_0%,#f1f3f5_58%,#e3e7eb_100%)] px-7 py-3.5 text-center text-stone-900 no-underline shadow-[inset_0_1px_0_rgb(255_255_255/90%),0_5px_0_#b1bac3,0_10px_18px_rgb(41_37_36/14%)] transition-[transform,box-shadow,filter] duration-150 hover:brightness-[1.03] active:translate-y-[3px] active:brightness-[.98] active:shadow-[inset_0_1px_0_rgb(255_255_255/70%),0_2px_0_#b1bac3,0_4px_8px_rgb(41_37_36/10%)]"
                  href={telephoneHref(data.manager.phone)}
                >
                  <img className="size-7" src={phoneImage} alt="" aria-hidden="true" />
                  담당자에게 전화하기
                </a>
              </>
            ) : (
              // <p className="mt-4 text-stone-600">등록된 전화번호가 없습니다.</p>
              <></>
            )}
          </>
        ) : (
          <p className="mt-4 text-stone-600">
            {data ? "아직 배정된 담당자가 없습니다." : "담당자 정보를 불러오고 있어요."}
          </p>
        )}
      </section>
    </>
  );
}
