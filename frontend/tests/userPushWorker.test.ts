import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";
import { resolve } from "node:path";
import { expect, it, vi } from "vitest";

const future = new Date(Date.now() + 60_000).toISOString();
type WorkerEvent = { data?: unknown; notification?: unknown; action?: string; waitUntil: (promise: Promise<unknown>) => void };

function worker(file = "sw.js") {
  const handlers: Record<string, (event: WorkerEvent) => void> = {};
  const postMessage = vi.fn();
  const showNotification = vi.fn<(title: string, options: NotificationOptions & { actions: unknown[] }) => Promise<void>>().mockResolvedValue(undefined);
  const openWindow = vi.fn<(url: string) => Promise<void>>().mockResolvedValue(undefined);
  runInNewContext(readFileSync(resolve("public", file), "utf8"), {
    URL, URLSearchParams, Date,
    self: {
      location: { origin: "https://example.com" },
      addEventListener: (name: string, handler: typeof handlers[string]) => { handlers[name] = handler; },
      registration: { showNotification },
      clients: { matchAll: async () => [{ url: "https://example.com/user", postMessage }], openWindow },
    },
  });
  async function push(payload: Record<string, unknown>) {
    let completion: Promise<unknown> | undefined;
    handlers.push({ data: { json: () => payload }, waitUntil: (promise) => { completion = promise; } });
    await completion;
    return showNotification.mock.calls[0] as unknown as [string, NotificationOptions & { actions: unknown[] }];
  }
  return { handlers, push, postMessage, openWindow };
}

it("응답이 필요한 알림만 브라우저와 페이지 양쪽에 기한을 포함한 응답 정보를 전달한다", async () => {
  const instance = worker();
  const [, options] = await instance.push({ notificationId: "1", expiresAt: future, title: "안전 확인", body: "상태 확인" });
  expect(options.actions).toHaveLength(2);
  expect(instance.postMessage).toHaveBeenCalledWith(expect.objectContaining({ notificationId: "1", expiresAt: future }));
  expect(new URL(options.data.url, "https://example.com").searchParams.get("expiresAt")).toBe(future);
});

it("응답 불필요 알림은 URL에 이전 번호가 있어도 응답 버튼을 표시하지 않는다", async () => {
  const instance = worker();
  const [, options] = await instance.push({ title: "생활 안내", url: "/user?notificationId=1&answer=yes" });
  expect(options.actions).toEqual([]);
  expect(options.data.url).toBe("https://example.com/user");
  expect(instance.postMessage.mock.calls[0][0].notificationId).toBeUndefined();
});

it("기한이 지난 알림에는 응답 버튼이 없고 이전 버튼을 눌러도 응답을 전달하지 않는다", async () => {
  const instance = worker();
  const [, options] = await instance.push({ notificationId: "1", expiresAt: new Date(Date.now() - 1000).toISOString() });
  expect(options.actions).toEqual([]);
  let completion: Promise<unknown> | undefined;
  instance.handlers.notificationclick({ notification: { data: options.data, close: vi.fn() }, action: "yes", waitUntil: (promise) => { completion = promise; } });
  await completion;
  const target = new URL(instance.openWindow.mock.calls[0][0]);
  expect(target.searchParams.has("answer")).toBe(false);
});

it("유효한 브라우저 응답 버튼은 같은 알림 번호와 만료 시각을 유지한다", async () => {
  const instance = worker();
  const [, options] = await instance.push({ notificationId: "1", expiresAt: future });
  let completion: Promise<unknown> | undefined;
  instance.handlers.notificationclick({ notification: { data: options.data, close: vi.fn() }, action: "no", waitUntil: (promise) => { completion = promise; } });
  await completion;
  const target = new URL(instance.openWindow.mock.calls[0][0]);
  expect(target.searchParams.get("notificationId")).toBe("1");
  expect(target.searchParams.get("expiresAt")).toBe(future);
  expect(target.searchParams.get("answer")).toBe("no");
});

it("테스트 서비스 워커도 안내 알림과 기한이 있는 응답 알림을 구분한다", async () => {
  const information = worker("user-push-test-sw.js");
  const [, infoOptions] = await information.push({ title: "생활 안내", body: "응답 불필요" });
  expect(infoOptions.actions).toEqual([]);
  expect(infoOptions.data.url).toBe("/user-push-test");
  const response = worker("user-push-test-sw.js");
  const [, responseOptions] = await response.push({ notificationId: "1", expiresAt: future });
  expect(responseOptions.actions).toHaveLength(2);
  expect(responseOptions.data.expiresAt).toBe(future);
});
