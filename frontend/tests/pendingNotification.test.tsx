import { act } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { usePendingNotification } from "../src/hooks/notifications/usePendingNotification";
import { isUnavailableNotificationError, readResponseNotification } from "../src/utils/userNotifications";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
const now = Date.parse("2026-09-26T10:00:00Z");
const expiresAt = new Date(now + 1000).toISOString();
let root: ReturnType<typeof createRoot>;
let serviceWorker: EventTarget;
let node: HTMLDivElement;
const originalServiceWorker = Object.getOwnPropertyDescriptor(navigator, "serviceWorker");

function Probe() {
  const pending = usePendingNotification();
  return <div>
    <output data-testid="pending">{pending.notificationId}</output>
    <output data-testid="information">{pending.information?.message}</output>
    <button onClick={() => pending.clearNotification("1")}>첫 알림 닫기</button>
  </div>;
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(now);
  serviceWorker = new EventTarget();
  Object.defineProperty(navigator, "serviceWorker", { configurable: true, value: serviceWorker });
  node = document.createElement("div");
  document.body.append(node);
  root = createRoot(node);
});

afterEach(async () => {
  await act(async () => root.unmount());
  vi.useRealTimers();
  if (originalServiceWorker) Object.defineProperty(navigator, "serviceWorker", originalServiceWorker);
  else Reflect.deleteProperty(navigator, "serviceWorker");
  document.body.innerHTML = "";
  window.history.replaceState(null, "", "/");
});

async function render(search = "") {
  window.history.replaceState(null, "", `/user${search}`);
  await act(async () => root.render(<BrowserRouter><Probe /></BrowserRouter>));
  await act(async () => vi.advanceTimersByTime(0));
}

async function receive(payload: Record<string, unknown>) {
  await act(async () => serviceWorker.dispatchEvent(new MessageEvent("message", { data: { type: "PUSH_RECEIVED", ...payload } })));
  await act(async () => vi.advanceTimersByTime(0));
}

it("서버의 실제 만료 시각에 응답창을 닫고 URL의 응답 정보를 제거한다", async () => {
  await render(`?notificationId=1&expiresAt=${encodeURIComponent(expiresAt)}`);
  await act(async () => vi.advanceTimersByTime(999));
  expect(node.querySelector('[data-testid="pending"]')?.textContent).toBe("1");
  await act(async () => vi.advanceTimersByTime(1));
  expect(node.querySelector('[data-testid="pending"]')?.textContent).toBe("");
  expect(window.location.search).toBe("");
  expect(node.textContent).toContain("안부 확인 응답 시간이 지났어요.");
});

it("응답용 메타데이터가 없는 정보 알림은 안내만 표시한다", async () => {
  await render();
  await receive({ title: "생활 안내", body: "응답하지 않아도 되는 안내입니다." });
  expect(node.querySelector('[data-testid="pending"]')?.textContent).toBe("");
  expect(node.textContent).toContain("응답하지 않아도 되는 안내입니다.");
});

it("브라우저에서 늦게 전달된 만료 알림과 기한이 없는 이전 링크는 응답창을 열지 않는다", async () => {
  await render("?notificationId=1&answer=yes");
  expect(window.location.search).toBe("");
  await receive({ notificationId: "2", expiresAt: new Date(now - 1).toISOString() });
  expect(node.querySelector('[data-testid="pending"]')?.textContent).toBe("");
});

it("백그라운드에서 복귀하면 타이머가 늦어져도 기한을 다시 확인한다", async () => {
  await render(`?notificationId=1&expiresAt=${encodeURIComponent(expiresAt)}`);
  vi.setSystemTime(now + 2000);
  await act(async () => window.dispatchEvent(new Event("focus")));
  expect(node.querySelector('[data-testid="pending"]')?.textContent).toBe("");
});

it("이전 알림의 응답 완료가 새 알림을 닫지 않고 닫힌 알림을 재수신해도 열지 않는다", async () => {
  await render();
  await receive({ notificationId: "1", expiresAt });
  await receive({ notificationId: "2", expiresAt });
  await act(async () => node.querySelector("button")?.click());
  expect(node.querySelector('[data-testid="pending"]')?.textContent).toBe("2");
  expect(new URLSearchParams(window.location.search).get("notificationId")).toBe("2");
  await receive({ notificationId: "1", expiresAt });
  expect(node.querySelector('[data-testid="pending"]')?.textContent).toBe("2");
});

it("기한 검증과 서버의 응답 불가 오류를 일반 네트워크 실패와 구분한다", () => {
  expect(readResponseNotification({ notificationId: "undefined", expiresAt })).toBeNull();
  expect(readResponseNotification({ notificationId: "1", expiresAt: "invalid" })).toBeNull();
  expect(isUnavailableNotificationError({ isAxiosError: true, response: { status: 409 } })).toBe(true);
  expect(isUnavailableNotificationError({ isAxiosError: true, response: { status: 404 } })).toBe(true);
  expect(isUnavailableNotificationError({ isAxiosError: true, response: { status: 500 } })).toBe(false);
  expect(isUnavailableNotificationError(new Error("network"))).toBe(false);
});
