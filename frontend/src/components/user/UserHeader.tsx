import { motion, useReducedMotion } from "motion/react";
import { useEffect, useId, useRef, useState } from "react";
import Brand from "../common/Brand";
import userIcon from "../../assets/user/user.svg";

type UserHeaderProps = {
  onGoHome: () => void;
  onLogout: () => void | Promise<void>;
  onOpenTestTools?: () => void;
};

export default function UserHeader({ onGoHome, onLogout, onOpenTestTools }: UserHeaderProps) {
  const reduceMotion = useReducedMotion();
  const [open, setOpen] = useState(false);
  const menuId = useId();
  const menuContainer = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!open) return;

    const closeFromOutside = (event: PointerEvent) => {
      if (!menuContainer.current?.contains(event.target as Node)) setOpen(false);
    };
    const closeFromKeyboard = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setOpen(false);
      trigger.current?.focus();
    };

    document.addEventListener("pointerdown", closeFromOutside);
    document.addEventListener("keydown", closeFromKeyboard);
    return () => {
      document.removeEventListener("pointerdown", closeFromOutside);
      document.removeEventListener("keydown", closeFromKeyboard);
    };
  }, [open]);

  return (
    <header className="flex items-center justify-between gap-3 px-5 py-5">
      <Brand />
      <div className="relative" ref={menuContainer}>
        <button
          ref={trigger}
          className="flex size-12 items-center justify-center rounded-full transition-colors hover:bg-stone-100 focus-visible:outline-3 focus-visible:outline-offset-2 focus-visible:outline-brand-500"
          type="button"
          aria-label="사용자 설정 메뉴"
          aria-controls={menuId}
          aria-expanded={open}
          aria-haspopup="menu"
          onClick={() => setOpen((value) => !value)}
        >
          <img className="size-8" alt="" src={userIcon} aria-hidden="true" />
        </button>
        {open && (
          <motion.div
            initial={reduceMotion ? false : { opacity: 0, y: -4 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.16 }}
            className="absolute top-[calc(100%+.5rem)] right-0 z-40 w-48 overflow-hidden rounded-2xl border border-stone-200 bg-white p-2 shadow-[0_12px_32px_rgb(41_37_36_/_16%)]"
            id={menuId}
            role="menu"
            aria-label="사용자 설정"
          >
            {onOpenTestTools && (
              <button
                className="w-full rounded-xl px-4 py-3 text-left text-lg font-semibold text-stone-800 transition-colors hover:bg-stone-100 focus-visible:outline-3 focus-visible:outline-brand-500"
                type="button"
                role="menuitem"
                onClick={() => {
                  setOpen(false);
                  onOpenTestTools();
                }}
              >
                테스트 도구 열기
              </button>
            )}
            <button
              className="w-full rounded-xl px-4 py-3 text-left text-lg font-semibold text-stone-800 transition-colors hover:bg-stone-100 focus-visible:outline-3 focus-visible:outline-brand-500"
              type="button"
              role="menuitem"
              onClick={() => {
                setOpen(false);
                onGoHome();
              }}
            >
              <span className="block">서비스 선택</span>
              <span className="block text-sm font-normal text-stone-500">메인으로 돌아가기</span>
            </button>
            <button
              className="mt-1 w-full rounded-xl px-4 py-3 text-left text-lg font-semibold text-red-700 transition-colors hover:bg-red-50 focus-visible:outline-3 focus-visible:outline-red-500"
              type="button"
              role="menuitem"
              onClick={() => {
                setOpen(false);
                void onLogout();
              }}
            >
              로그아웃
            </button>
          </motion.div>
        )}
      </div>
    </header>
  );
}
