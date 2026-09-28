import { motion, useReducedMotion } from "motion/react";
import type { FormEvent } from "react";
import { getApiErrorMessage } from "../../api/client";
import { useRegisterSubjectMutation } from "../../hooks/api";
import type { SubjectRegistration } from "../../types/monitoring";
import { todayInSeoul } from "../../utils/format";


const inputClass =
  "h-12 rounded-xl border border-stone-300 px-4 outline-none focus:border-brand-500 focus:ring-3 focus:ring-brand-100";

export default function RegistrationForm({
  onDone,
  onCancel,
}: {
  onDone: () => void;
  onCancel: () => void;
}) {
  const registration = useRegisterSubjectMutation();
  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const values = Object.fromEntries(new FormData(event.currentTarget)) as SubjectRegistration;
    registration.mutate(values, { onSuccess: onDone });
  };
  const reduceMotion = useReducedMotion();

  return (
    <motion.section
      initial={reduceMotion ? false : { opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, ease: "easeOut" }}
      className="rounded-xl border border-stone-200/70 bg-white p-7"
    >
      <div className="flex items-center justify-between">
        <h2 className="flex items-center gap-2 text-xl font-bold">
          대상자 등록
        </h2>
        <button
          className="rounded-lg px-3 py-2 font-semibold text-stone-600 hover:bg-stone-100"
          onClick={onCancel}
          disabled={registration.isPending}
        >
          닫기
        </button>
      </div>
      <p className="mt-2 text-stone-500">등록된 가구 ID를 사용해 담당 대상자를 연결합니다.</p>
      <form className="mt-6 grid gap-4 md:grid-cols-2" onSubmit={submit}>
        <label className="grid gap-2 font-semibold text-stone-700">
          이름
          <input className={inputClass} name="name" required maxLength={50} />
        </label>
        <label className="grid gap-2 font-semibold text-stone-700">
          생년월일
          <input
            className={inputClass}
            name="birthDate"
            type="date"
            required
            max={todayInSeoul()}
          />
        </label>
        <label className="grid gap-2 font-semibold text-stone-700">
          전화번호
          <input
            className={inputClass}
            name="phone"
            type="tel"
            required
            pattern="0[0-9]{8,10}"
            placeholder="01012345678"
          />
        </label>
        <label className="grid gap-2 font-semibold text-stone-700">
          가구 ID
          <input className={inputClass} name="householdId" required maxLength={10} />
        </label>
        <label className="grid gap-2 font-semibold text-stone-700">
          주소
          <input className={inputClass} name="address" required maxLength={255} />
        </label>
        <label className="grid gap-2 font-semibold text-stone-700">
          상세 주소
          <input className={inputClass} name="addressDetail" maxLength={100} />
        </label>
        <label className="grid gap-2 font-semibold text-stone-700 md:col-span-2">
          담당자 메모
          <textarea
            className="rounded-xl border border-stone-300 p-4 outline-none focus:border-brand-500 focus:ring-3 focus:ring-brand-100"
            name="managerMemo"
            maxLength={1000}
            rows={3}
          />
        </label>
        {registration.isError && (
          <p className="rounded-xl bg-red-50 p-4 text-red-700 md:col-span-2" role="alert">
            {getApiErrorMessage(registration.error)}
          </p>
        )}
        <button
          className="h-12 rounded-xl bg-brand-500 font-bold text-white hover:bg-brand-600 disabled:bg-stone-300"
          disabled={registration.isPending}
        >
          {registration.isPending ? "등록 중…" : "대상자 등록"}
        </button>
      </form>
    </motion.section>
  );
}
