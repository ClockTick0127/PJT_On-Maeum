import { motion, useReducedMotion } from "motion/react";
import { useState } from "react";
import type { FormEvent } from "react";
import { login } from "../api/auth";
import { getApiErrorMessage } from "../api/client";
import { saveSession } from "../api/tokenStorage";
import LoginCard from "../components/auth/LoginCard";

export default function LoginPage({ role }: { role: "staff" | "user" }) {
  const reduceMotion = useReducedMotion();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    setBusy(true);
    setError("");
    const keepSignedIn = form.get("keepSignedIn") === "true";
    login(String(form.get("email")).trim(), String(form.get("password")))
      .then((tokens) => saveSession(tokens, keepSignedIn, role))
      .catch((cause: unknown) => setError(getApiErrorMessage(cause)))
      .finally(() => setBusy(false));
  };
  return (
    <motion.main
      initial={reduceMotion ? false : { opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: role === "user" ? 0.18 : 0.25 }}
      className={`grid min-h-screen place-items-center p-5 ${role === "user" ? "bg-stone-50 pb-[calc(7rem+env(safe-area-inset-bottom))] text-xl leading-[1.6] text-stone-800 [overflow-wrap:anywhere]" : "bg-stone-100"}`}
    >
      <LoginCard role={role} busy={busy} error={error} onSubmit={submit} />
    </motion.main>
  );
}
