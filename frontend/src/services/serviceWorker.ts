export async function waitForActiveServiceWorker(registration: ServiceWorkerRegistration) {
  if (registration.active?.state === "activated") return registration;
  const worker = registration.installing ?? registration.waiting ?? registration.active;
  if (!worker) throw new Error("알림 서비스 워커를 준비하지 못했습니다. 다시 시도해 주세요.");

  await new Promise<void>((resolve, reject) => {
    const finish = (error?: Error) => {
      window.clearTimeout(timer);
      worker.removeEventListener("statechange", checkState);
      if (error) reject(error);
      else resolve();
    };
    const checkState = () => {
      if (worker.state === "activated") finish();
      else if (worker.state === "redundant") finish(new Error("알림 서비스 워커 활성화에 실패했습니다."));
    };
    const timer = window.setTimeout(
      () => finish(new Error("알림 서비스 워커 준비 시간이 초과됐습니다. 다시 시도해 주세요.")),
      10_000,
    );
    worker.addEventListener("statechange", checkState);
    checkState();
  });
  return registration;
}

export async function getAppServiceWorkerRegistration() {
  const existing = await navigator.serviceWorker.getRegistration("/");
  const registration = existing ?? await navigator.serviceWorker.register("/sw.js", { scope: "/" });
  return waitForActiveServiceWorker(registration);
}
