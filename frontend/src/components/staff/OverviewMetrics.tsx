import { motion, useReducedMotion } from "motion/react";
import type { Subject, Risk } from "../../types/monitoring";
import { riskLabels } from "../../types/monitoring";

export default function OverviewMetrics({
  subjects,
  loading,
}: {
  subjects: Subject[];
  loading: boolean;
}) {
  const metrics = [
    ...(["DANGER", "WARNING", "NORMAL"] as Risk[]).map((risk) => ({
      label: riskLabels[risk],
      value: subjects.filter((subject) => subject.riskLevel === risk).length,
      valueStyle:
        risk === "DANGER"
          ? "text-red-600"
          : risk === "WARNING"
            ? "text-brand-600"
            : "text-stone-700",
    })),
  ];

  const reduceMotion = useReducedMotion();

  return (
    <motion.section
      initial={reduceMotion ? false : { opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.2 }}
      className="space-y-4"
      aria-label="대상자 현황"
    >
      <p className="flex items-baseline gap-2 text-base font-medium text-stone-600">
        담당 대상자
        <strong className="text-2xl font-semibold tabular-nums text-stone-900">{loading ? "—" : subjects.length}</strong>
        <span>명</span>
      </p>
      <div className="grid grid-cols-3 divide-x divide-stone-200 rounded-xl border border-stone-200 bg-white py-5">
      {metrics.map((metric) => (
        <article
          className="px-5 md:px-7"
          key={metric.label}
        >
          <span className="text-base font-medium text-stone-600">{metric.label}</span>
          <strong className={`mt-2 block text-3xl font-semibold tabular-nums ${metric.valueStyle}`}>
            {loading ? "—" : metric.value}
            <small className="ml-1 text-base font-semibold text-stone-500">명</small>
          </strong>
        </article>
      ))}
      </div>
    </motion.section>
  );
}
