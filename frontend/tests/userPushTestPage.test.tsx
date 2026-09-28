import { act } from "react";
import type { ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import UserPushTestPage from "../src/pages/UserPushTestPage";

vi.mock("../src/components/user/UserHeader", () => ({ default: () => null }));
vi.mock("../src/components/user/UserHomeTab", () => ({ default: () => null }));
vi.mock("motion/react", () => ({ motion: { div: ({ children }: { children: ReactNode }) => <div>{children}</div> }, useReducedMotion: () => true }));
vi.mock("../src/components/user/UserStatusBar", () => ({ default: ({ notificationPending }: { notificationPending: boolean }) => notificationPending ? <p>지금 상태를 알려주세요</p> : null }));
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
let root: ReturnType<typeof createRoot>;
let node: HTMLDivElement;
let showNotification: ReturnType<typeof vi.fn>;
let register: ReturnType<typeof vi.fn>;
let worker: EventTarget & { state: string };
const originalServiceWorker = Object.getOwnPropertyDescriptor(navigator, "serviceWorker");

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubGlobal("isSecureContext", true);
  vi.stubGlobal("Notification", { permission: "granted" });
  worker = Object.assign(new EventTarget(), { state: "activated" });
  showNotification = vi.fn(async () => undefined);
  register = vi.fn(async () => ({ active: worker, showNotification }));
  Object.defineProperty(navigator, "serviceWorker", {
    configurable: true,
    value: Object.assign(new EventTarget(), { register }),
  });
  window.history.replaceState(null, "", "/user-push-test");
  node = document.createElement("div");
  document.body.append(node);
  root = createRoot(node);
});
afterEach(async () => {
  await act(async () => root.unmount());
  vi.useRealTimers();
  vi.unstubAllGlobals();
  if (originalServiceWorker) Object.defineProperty(navigator, "serviceWorker", originalServiceWorker);
  else Reflect.deleteProperty(navigator, "serviceWorker");
  document.body.innerHTML = "";
  window.history.replaceState(null, "", "/");
});
async function render() {
  await act(async () => root.render(<BrowserRouter><UserPushTestPage /></BrowserRouter>));
}
async function clickNotification() {
  const button = [...node.querySelectorAll("button")].find((element) => element.textContent === "브라우저 알림 표시");
  expect(button).toBeDefined();
  await act(async () => button?.click());
}

it("브라우저 알림 버튼은 테스트 워커에 표시 요청을 보내고 같은 기한으로 응답창을 연다", async () => {
  await render();
  await clickNotification();
  expect(register).toHaveBeenCalledWith("/user-push-test-sw.js", { scope: "/user-push-test" });
  expect(showNotification).toHaveBeenCalledOnce();
  const [title, options] = showNotification.mock.calls[0];
  expect(title).toBe("On:마음 안심 알림 · 테스트");
  expect(options.actions).toHaveLength(2);
  expect(new URLSearchParams(window.location.search).get("expiresAt")).toBe(options.data.expiresAt);
  expect(node.textContent).toContain("지금 상태를 알려주세요");
  await act(async () => vi.advanceTimersByTime(30_000));
  expect(node.textContent).not.toContain("지금 상태를 알려주세요");
});

it("버튼을 반복해서 눌러도 서로 다른 알림 태그로 표시를 요청한다", async () => {
  await render();
  await clickNotification();
  await act(async () => vi.advanceTimersByTime(1));
  await clickNotification();
  expect(showNotification).toHaveBeenCalledTimes(2);
  expect(showNotification.mock.calls[0][1].tag).not.toBe(showNotification.mock.calls[1][1].tag);
});

it("서비스 워커 활성화가 끝나기 전에는 표시 요청을 보내지 않는다", async () => {
  worker.state = "activating";
  await render();
  await clickNotification();
  expect(showNotification).not.toHaveBeenCalled();
  await act(async () => {
    worker.state = "activated";
    worker.dispatchEvent(new Event("statechange"));
  });
  expect(showNotification).toHaveBeenCalledOnce();
});

it("브라우저 표시 요청이 실패하면 성공으로 처리하지 않고 오류를 화면에 표시한다", async () => {
  showNotification.mockRejectedValueOnce(new Error("브라우저 알림 표시 실패"));
  await render();
  await clickNotification();
  expect(node.textContent).toContain("브라우저 알림 표시 실패");
  expect(node.textContent).not.toContain("지금 상태를 알려주세요");
});
