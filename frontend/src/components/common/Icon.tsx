export type IconName =
  | "grid"
  | "users"
  | "bell"
  | "history"
  | "shield"
  | "home"
  | "walk"
  | "clock"
  | "device"
  | "arrow"
  | "back"
  | "check"
  | "phone"
  | "search"
  | "trend"
  | "close";

export default function Icon({ name, className = "" }: { name: IconName; className?: string }) {
  const paths = {
    grid: (
      <>
        <rect x="3" y="3" width="7" height="7" rx="2" />
        <rect x="14" y="3" width="7" height="7" rx="2" />
        <rect x="3" y="14" width="7" height="7" rx="2" />
        <rect x="14" y="14" width="7" height="7" rx="2" />
      </>
    ),
    users: (
      <>
        <circle cx="9" cy="8" r="3" />
        <path d="M3 21v-3a6 6 0 0 1 12 0v3M16 5a3 3 0 0 1 0 6M18 15a5 5 0 0 1 3 5" />
      </>
    ),
    bell: (
      <>
        <path d="M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4" />
      </>
    ),
    history: (
      <>
        <path d="M3 11a9 9 0 1 1 2 7M3 4v7h7M12 7v5l3 2" />
      </>
    ),
    shield: (
      <>
        <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10" />
        <path d="m8 12 3 3 5-6" />
      </>
    ),
    home: (
      <>
        <path d="m3 10 9-7 9 7v11H3ZM9 21v-8h6v8" />
      </>
    ),
    walk: (
      <>
        <circle cx="13" cy="4" r="2" />
        <path d="m8 22 3-7-2-5 4-3 3 5h4M4 12l4-2M11 15l5 3 1 4" />
      </>
    ),
    clock: (
      <>
        <circle cx="12" cy="12" r="9" />
        <path d="M12 7v5l3 2" />
      </>
    ),
    device: (
      <>
        <rect x="5" y="3" width="14" height="18" rx="3" />
        <path d="M9 7h6M9 11h6M11 17h2" />
      </>
    ),
    arrow: <path d="M4 12h16m-6-6 6 6-6 6" />,
    back: <path d="M20 12H4m6-6-6 6 6 6" />,
    check: <path d="m5 12 4 4L19 6" />,
    phone: <path d="M8 3H4v3a15 15 0 0 0 14 15h3v-5l-5-2-2 2a12 12 0 0 1-6-6l2-2Z" />,
    search: (
      <>
        <circle cx="10" cy="10" r="7" />
        <path d="m15 15 6 6" />
      </>
    ),
    trend: <path d="m3 17 6-6 4 4 8-11m-6 0h6v6" />,
    close: <path d="m6 6 12 12M6 18 18 6" />,
  };
  return (
    <svg
      className={`h-5 w-5 shrink-0 ${className}`}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {paths[name]}
    </svg>
  );
}
