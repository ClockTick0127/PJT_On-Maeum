import { Link } from "react-router-dom";
import Icon from "../common/Icon";

const roles = [
  {
    to: "/staff",
    title: "복지담당자",
    description: "담당 대상자의 상태와 위험 신호를 확인해요.",
    icon: "users" as const,
  },
  {
    to: "/user",
    title: "복지대상자",
    description: "외출을 알리고 담당자와 연결해요.",
    icon: "home" as const,
  },
];

export default function RoleSelection() {
  return (
    <div className="grid gap-4 md:grid-cols-2">
      {roles.map((role) => (
        <Link
          className="group flex items-center gap-4 rounded-2xl border border-stone-200 bg-stone-50 p-5 transition hover:-translate-y-0.5 hover:border-brand-500 hover:shadow-lg"
          to={role.to}
          key={role.to}
        >
          <span className="grid h-12 w-12 place-items-center rounded-xl bg-brand-500 text-white">
            <Icon name={role.icon} />
          </span>
          <span className="flex-1">
            <strong className="block text-lg text-stone-800">{role.title}</strong>
            <small className="mt-1 block leading-5 text-stone-500">{role.description}</small>
          </span>
          <Icon className="text-brand-600 transition group-hover:translate-x-1" name="arrow" />
        </Link>
      ))}
    </div>
  );
}
