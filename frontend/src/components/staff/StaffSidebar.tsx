import { Link } from "react-router-dom";
import Brand from "../common/Brand";
import Icon from "../common/Icon";
import managerIcon from "../../assets/user/manager.svg";
import { staffNavigation } from "./staffNavigation";
import type { StaffView } from "./staffNavigation";

type StaffSidebarProps = {
  view: StaffView;
  subjectId?: string;
  manager?: {
    name: string;
    email: string;
  };
  onLogout: () => void;
  profileError?: boolean;
  onRetryProfile?: () => void;
  basePath?: string;
};

export default function StaffSidebar({ view, subjectId, manager, onLogout, profileError, onRetryProfile, basePath = "/staff" }: StaffSidebarProps) {
  return (
    <aside className="flex items-center gap-4 border-b border-stone-200/70 bg-[#fcfcfb] p-4 lg:fixed lg:inset-y-0 lg:z-20 lg:w-[250px] lg:flex-col lg:items-stretch lg:border-b-0 lg:p-7">
      <Brand />
      <nav className="ml-auto flex gap-1 lg:mt-14 lg:ml-0 lg:grid lg:gap-2" aria-label="담당자 메뉴">
        {staffNavigation.map((item) => {
          const selected = subjectId ? item.key === "subjects" : view === item.key;
          return (
            <Link
              className={`flex items-center gap-3 rounded-lg px-4 py-3.5 text-base font-semibold transition-colors ${selected ? "bg-brand-500 text-white" : "text-stone-500 hover:bg-stone-100 hover:text-stone-800"}`}
              to={`${basePath}?view=${item.key}`}
              key={item.key}
              aria-current={selected ? "page" : undefined}
            >
              <Icon name={item.icon} />
              <span className="hidden lg:inline">{item.label}</span>
            </Link>
          );
        })}
      </nav>
      <div className="mt-auto hidden pt-6 lg:block">
        <div className="flex items-center gap-3">
          <img className="size-10 shrink-0" src={managerIcon} alt="" aria-hidden="true" />
          <div className="min-w-0">
            <p className="truncate font-bold text-stone-800">{manager ? manager.name || "이름 미등록" : profileError ? "계정 조회 실패" : "계정 확인 중"}</p>
            {manager && <p className="truncate text-base text-stone-500">{manager.email}</p>}
          </div>
        </div>
        {profileError && <button className="mt-2 text-base font-semibold text-brand-700 underline" onClick={onRetryProfile}>계정 정보 다시 확인</button>}
        <button
          className="mt-4 rounded-md py-2 text-base font-semibold text-stone-500 transition-colors hover:bg-stone-50 hover:text-brand-700"
          onClick={onLogout}
        >
          로그아웃
        </button>
      </div>
    </aside>
  );
}
