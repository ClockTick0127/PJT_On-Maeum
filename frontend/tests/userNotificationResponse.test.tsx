import { act } from "react";
import type { ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import UserPage from "../src/pages/UserPage";

const { mutate } = vi.hoisted(() => ({ mutate: vi.fn() }));
vi.mock("../src/hooks/api", () => ({
  useMyDashboardQuery: () => ({ data: { name: "박정수", awayMode: { enabled: false } }, isError: false }),
  useAwayModeMutation: () => ({ isPending: false }),
  useNotificationResponseMutation: () => ({ isPending: false, mutateAsync: mutate }),
}));
vi.mock("../src/hooks/notifications/usePushSubscription", () => ({ usePushSubscription: () => ({ status: "subscribed" }) }));
vi.mock("../src/components/user/UserHeader", () => ({ default: () => null }));
vi.mock("../src/components/user/UserHomeTab", () => ({ default: () => null }));
vi.mock("motion/react", () => ({ motion: { div: ({ children }: { children: ReactNode }) => <div>{children}</div> }, useReducedMotion: () => true }));
vi.mock("../src/components/user/UserStatusBar", () => ({
  default: ({ notificationPending, onAnswerNotification }: { notificationPending: boolean; onAnswerNotification: (answer: "yes") => void }) => notificationPending ? <button onClick={() => onAnswerNotification("yes")}>도움이 필요해요</button> : <p>응답창 닫힘</p>,
}));

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
let root: ReturnType<typeof createRoot>;
let node: HTMLDivElement;
beforeEach(() => {
  vi.useFakeTimers();
  const params = new URLSearchParams({ notificationId: "1", expiresAt: new Date(Date.now() + 60_000).toISOString() });
  window.history.replaceState(null, "", `/user?${params}`);
  node = document.createElement("div");
  document.body.append(node);
  root = createRoot(node);
});
afterEach(async () => {
  await act(async () => root.unmount());
  vi.useRealTimers();
  mutate.mockReset();
  document.body.innerHTML = "";
  window.history.replaceState(null, "", "/");
});
async function answer() {
  await act(async () => root.render(<BrowserRouter><UserPage /></BrowserRouter>));
  await act(async () => vi.advanceTimersByTime(0));
  await act(async () => node.querySelector("button")?.click());
}

it("서버가 응답 기한 만료로 거절하면 오류 안내와 함께 응답창을 닫는다", async () => {
  mutate.mockRejectedValue({ isAxiosError: true, response: { status: 409, data: { message: "응답 기한이 지났습니다." } } });
  await answer();
  expect(mutate).toHaveBeenCalledWith({ notificationId: "1", answer: "yes" });
  expect(node.textContent).toContain("응답창 닫힘");
  expect(window.location.search).toBe("");
});

it("일반 전송 실패는 기한 안에서 다시 응답할 수 있도록 응답창을 유지한다", async () => {
  mutate.mockRejectedValue(new Error("네트워크 오류"));
  await answer();
  expect(node.textContent).toContain("도움이 필요해요");
  expect(new URLSearchParams(window.location.search).get("notificationId")).toBe("1");
});
