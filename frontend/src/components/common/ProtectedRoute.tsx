import { useState } from "react";
import type { ReactNode } from "react";
import { Link, Outlet, useNavigate } from "react-router-dom";
import { clearSession } from "../../api/tokenStorage";
import type { SessionRole } from "../../api/tokenStorage";
import { useSession, useSessionRole } from "../../hooks/useSession";
import { unsubscribeFromPush } from "../../services/pushSubscription";
import Brand from "./Brand";

type ProtectedRouteProps = {
  fallback: ReactNode;
  role: SessionRole;
};

const roleLabels: Record<SessionRole, string> = {
  staff: "복지담당자",
  user: "복지대상자",
};

export default function ProtectedRoute({ fallback, role }: ProtectedRouteProps) {
  const navigate = useNavigate();
  const hasActiveSession = useSession();
  const activeRole = useSessionRole();
  const [loggingOut, setLoggingOut] = useState(false);

  if (!hasActiveSession) return fallback;
  if (activeRole === role) return <Outlet />;

  const logout = async () => {
    setLoggingOut(true);
    await Promise.allSettled([unsubscribeFromPush()]);
    clearSession();
    navigate(role === "staff" ? "/staff" : "/user", { replace: true });
  };

  const activeRoleLabel = activeRole ? roleLabels[activeRole] : "기존";
  const requestedRoleLabel = roleLabels[role];

  return (
    <main className="grid min-h-screen place-items-center bg-stone-100 p-5 text-stone-800">
      <section className="w-full max-w-lg rounded-3xl border border-stone-200 bg-white p-7 shadow-lg md:p-9">
        <Brand />
        <h1 className="mt-2 text-3xl font-extrabold">
          {activeRoleLabel} 계정으로 로그인되어 있어요
        </h1>
        <p className="mt-4 leading-7 text-stone-600">
          {requestedRoleLabel} 서비스를 이용하려면 현재 계정에서 로그아웃한 뒤 다시 로그인해 주세요.
        </p>
        <button
          className="mt-8 flex min-h-14 w-full items-center justify-center rounded-xl bg-brand-500 px-5 py-3 font-bold text-white transition-colors hover:bg-brand-600 disabled:cursor-wait disabled:bg-stone-300"
          disabled={loggingOut}
          onClick={logout}
        >
          {loggingOut ? "로그아웃 중…" : `로그아웃`}
        </button>
        <Link
          className="mt-5 block text-center font-semibold text-stone-500 hover:text-brand-700"
          to="/"
        >
          서비스 선택으로 돌아가기
        </Link>
      </section>
    </main>
  );
}
