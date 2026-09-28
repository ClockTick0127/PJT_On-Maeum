import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { waitForActiveServiceWorker } from "../src/services/serviceWorker";

const { registerPush, deletePush } = vi.hoisted(() => ({ registerPush: vi.fn(), deletePush: vi.fn() }));
vi.mock("../src/api/monitoring", () => ({ registerPushSubscription: registerPush, deletePushSubscription: deletePush }));
const originalServiceWorker = Object.getOwnPropertyDescriptor(navigator, "serviceWorker");
const key = new Uint8Array([1, 2, 3]);

function subscription() {
  return {
    endpoint: "https://push.example.com/production", expirationTime: null,
    options: { applicationServerKey: key.buffer },
    toJSON: () => ({ keys: { p256dh: "key", auth: "auth" } }),
    unsubscribe: vi.fn(async () => true),
  };
}

function install(existing = subscription()) {
  const root = {
    active: { state: "activated" },
    pushManager: { getSubscription: vi.fn(async (): Promise<ReturnType<typeof subscription> | null> => existing), subscribe: vi.fn(async () => subscription()) },
  };
  const testWorker = { pushManager: { subscribe: vi.fn(), getSubscription: vi.fn() } };
  const serviceWorker = {
    ready: Promise.resolve(testWorker),
    getRegistration: vi.fn(async (): Promise<typeof root | undefined> => root),
    register: vi.fn(async () => root),
  };
  Object.defineProperty(navigator, "serviceWorker", { configurable: true, value: serviceWorker });
  return { root, testWorker, serviceWorker, existing };
}

beforeEach(() => {
  vi.resetModules();
  vi.stubEnv("VITE_WEB_PUSH_VAPID_PUBLIC_KEY", "AQID");
  vi.stubGlobal("Notification", { permission: "granted" });
  vi.stubGlobal("PushManager", function PushManager() {});
  registerPush.mockResolvedValue(undefined);
  deletePush.mockResolvedValue(undefined);
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  registerPush.mockReset();
  deletePush.mockReset();
  if (originalServiceWorker) Object.defineProperty(navigator, "serviceWorker", originalServiceWorker);
  else Reflect.deleteProperty(navigator, "serviceWorker");
});

it("테스트 워커가 문서를 제어해도 운영 구독을 재사용하고 운영 서버에 등록한다", async () => {
  const { root, testWorker, serviceWorker, existing } = install();
  const { synchronizePushSubscription } = await import("../src/services/pushSubscription");
  await expect(synchronizePushSubscription(false)).resolves.toBe("subscribed");
  expect(serviceWorker.getRegistration).toHaveBeenCalledWith("/");
  expect(root.pushManager.subscribe).not.toHaveBeenCalled();
  expect(testWorker.pushManager.getSubscription).not.toHaveBeenCalled();
  expect(testWorker.pushManager.subscribe).not.toHaveBeenCalled();
  expect(registerPush).toHaveBeenCalledWith({ endpoint: existing.endpoint, expirationTime: null, keys: { p256dh: "key", auth: "auth" } });
});

it("운영 워커가 없는 경우 운영 스크립트와 범위로 등록한 후 새 구독을 만든다", async () => {
  const { root, serviceWorker } = install();
  serviceWorker.getRegistration.mockResolvedValueOnce(undefined);
  root.pushManager.getSubscription.mockResolvedValueOnce(null);
  const { synchronizePushSubscription } = await import("../src/services/pushSubscription");
  await expect(synchronizePushSubscription(false)).resolves.toBe("subscribed");
  expect(serviceWorker.register).toHaveBeenCalledWith("/sw.js", { scope: "/" });
  expect(root.pushManager.subscribe).toHaveBeenCalledWith({ userVisibleOnly: true, applicationServerKey: key });
});

it("만료된 기존 구독은 해제한 후 새로 구독한다", async () => {
  const { existing, root } = install();
  Object.assign(existing, { expirationTime: Date.now() - 1 });
  const { synchronizePushSubscription } = await import("../src/services/pushSubscription");
  await expect(synchronizePushSubscription(false)).resolves.toBe("subscribed");
  expect(existing.unsubscribe).toHaveBeenCalledOnce();
  expect(root.pushManager.subscribe).toHaveBeenCalledOnce();
});

it("VAPID 키가 바뀐 구독도 새 키로 교체한다", async () => {
  const { existing, root } = install();
  existing.options.applicationServerKey = new Uint8Array([4, 5, 6]).buffer;
  const { synchronizePushSubscription } = await import("../src/services/pushSubscription");
  await expect(synchronizePushSubscription(false)).resolves.toBe("subscribed");
  expect(existing.unsubscribe).toHaveBeenCalledOnce();
  expect(root.pushManager.subscribe).toHaveBeenCalledWith({ userVisibleOnly: true, applicationServerKey: key });
});

it("로그아웃은 등록 완료를 기다린 뒤 운영 구독의 서버 삭제와 브라우저 해제를 수행한다", async () => {
  const { existing, serviceWorker } = install();
  let finishRegistration: () => void = () => {};
  registerPush.mockReturnValueOnce(new Promise<void>((resolve) => { finishRegistration = resolve; }));
  const module = await import("../src/services/pushSubscription");
  const synchronization = module.synchronizePushSubscription(false);
  await vi.waitFor(() => expect(registerPush).toHaveBeenCalledOnce());
  const logout = module.unsubscribeFromPush();
  expect(deletePush).not.toHaveBeenCalled();
  finishRegistration();
  await synchronization;
  await logout;
  expect(serviceWorker.getRegistration).toHaveBeenLastCalledWith("/");
  expect(deletePush).toHaveBeenCalledWith(existing.endpoint);
  expect(existing.unsubscribe).toHaveBeenCalledOnce();
  expect(deletePush.mock.invocationCallOrder[0]).toBeLessThan(existing.unsubscribe.mock.invocationCallOrder[0]);
  await module.synchronizePushSubscription(false);
  expect(registerPush).toHaveBeenCalledOnce();
});

it("활성화 진행 중인 서비스 워커는 완료될 때까지 기다린다", async () => {
  const worker = Object.assign(new EventTarget(), { state: "activating" });
  const registration = { active: worker } as unknown as ServiceWorkerRegistration;
  const pending = waitForActiveServiceWorker(registration);
  worker.state = "activated";
  worker.dispatchEvent(new Event("statechange"));
  await expect(pending).resolves.toBe(registration);
});

it("활성화가 멈추면 무한 대기 대신 오류를 반환한다", async () => {
  vi.useFakeTimers();
  const worker = Object.assign(new EventTarget(), { state: "installing" });
  const pending = waitForActiveServiceWorker({ installing: worker } as unknown as ServiceWorkerRegistration);
  const assertion = expect(pending).rejects.toThrow("준비 시간이 초과");
  await vi.advanceTimersByTimeAsync(10_000);
  await assertion;
});
