import type { FormEvent } from "react";
import { Link } from "react-router-dom";
import Brand from "../common/Brand";
import Icon from "../common/Icon";

type LoginCardProps = {
  role: "staff" | "user";
  busy: boolean;
  error: string;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
};

export default function LoginCard({ role, busy, error, onSubmit }: LoginCardProps) {
  return (
    <section
      className={
        role === "user"
          ? "w-full max-w-md rounded-3xl border border-stone-200 bg-white p-6 shadow-[0_2px_10px_rgb(15_23_42_/_3%)] max-[359px]:p-5"
          : "w-full max-w-md rounded-3xl border border-stone-200 bg-white p-7 shadow-xl md:p-9"
      }
    >
      <Brand />
      <p className="mt-10 text-sm font-bold text-brand-700">일상을 잇는 안심 돌봄</p>
      <h1 className="mt-2 text-3xl font-extrabold text-stone-800">
        {role === "staff" ? "복지담당자" : "복지대상자"} 로그인
      </h1>
      <p className="mt-2 text-stone-500">등록된 계정으로 로그인해 주세요.</p>
      <form
        className="mt-8 grid gap-5"
        name={`${role}-login`}
        action="/api/auth/login"
        method="post"
        autoComplete="on"
        onSubmit={onSubmit}
      >
        <label className="grid gap-2 font-semibold text-stone-700" htmlFor={`${role}-email`}>
          이메일
          <input
            id={`${role}-email`}
            className="h-13 rounded-xl border border-stone-300 px-4 outline-none focus:border-brand-500 focus:ring-3 focus:ring-brand-100"
            name="email"
            type="email"
            inputMode="email"
            autoComplete="username"
            autoCapitalize="none"
            spellCheck={false}
            required
          />
        </label>
        <label className="grid gap-2 font-semibold text-stone-700" htmlFor={`${role}-password`}>
          비밀번호
          <input
            id={`${role}-password`}
            className="h-13 rounded-xl border border-stone-300 px-4 outline-none focus:border-brand-500 focus:ring-3 focus:ring-brand-100"
            name="password"
            type="password"
            autoComplete="current-password"
            required
          />
        </label>
        <label
          className="flex min-h-12 cursor-pointer items-center gap-3 font-semibold text-stone-700"
          htmlFor={`${role}-keep-signed-in`}
        >
          <input
            id={`${role}-keep-signed-in`}
            className="h-5 w-5 shrink-0 accent-brand-500"
            name="keepSignedIn"
            type="checkbox"
            value="true"
          />
          로그인 상태 유지
        </label>
        {error && (
          <p className="rounded-xl bg-red-50 p-4 text-sm text-red-700" role="alert">
            {error}
          </p>
        )}
        <button
          className={
            role === "user"
              ? "flex min-h-16 w-full translate-y-0 cursor-pointer items-center justify-center gap-2.5 rounded-2xl border border-[#e67830] bg-[linear-gradient(180deg,#f58b40_0%,#f37929_58%,#e96e24_100%)] px-4 py-3.5 text-center text-2xl leading-6 font-semibold text-white shadow-[0_4px_0_#c96524,0_7px_14px_rgb(243_121_41_/_20%)] transition-[transform,box-shadow,filter] duration-150 hover:not-disabled:brightness-[1.02] active:not-disabled:translate-y-[3px] active:not-disabled:brightness-[.97] active:not-disabled:shadow-[0_1px_0_#c96524,0_3px_6px_rgb(74_54_35_/_8%)] disabled:cursor-not-allowed disabled:border-stone-200 disabled:bg-none disabled:bg-stone-200 disabled:text-stone-600 disabled:shadow-none"
              : "flex h-13 items-center justify-center gap-2 rounded-xl bg-brand-500 font-bold text-white transition hover:bg-brand-600 disabled:bg-stone-300"
          }
          type="submit"
          disabled={busy}
        >
          {busy ? "로그인 중…" : "로그인하기"}
          <Icon name="arrow" />
        </button>
      </form>
      <Link className="mt-6 block text-center font-semibold text-brand-700 hover:underline" to="/">
        서비스 선택으로 돌아가기
      </Link>
    </section>
  );
}
