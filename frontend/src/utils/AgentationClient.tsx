"use client";

import { Agentation } from "agentation";

export default function AgentationClient() {
  // 개발환경에서만 노출 (프로덕션 제외)
  if (process.env.NODE_ENV !== "development") return null;

  return <Agentation />;
}
