import { useCallback, useEffect, useState } from "react";

type LocationStatus = "idle" | "requesting" | "active" | "denied" | "error" | "unsupported";

export function useLocationService() {
  const supported = window.isSecureContext && "geolocation" in navigator;
  const [status, setStatus] = useState<LocationStatus>(supported ? "idle" : "unsupported");
  const [enabled, setEnabled] = useState(false);
  const [attempt, setAttempt] = useState(0);
  const [position, setPosition] = useState<GeolocationPosition | null>(null);
  const [error, setError] = useState("");

  const enable = useCallback(() => {
    if (!supported) return;
    setError("");
    setStatus("requesting");
    setEnabled(true);
    setAttempt((value) => value + 1);
  }, [supported]);

  useEffect(() => {
    if (!supported || !navigator.permissions) return;
    let active = true;
    let permission: PermissionStatus | undefined;
    const updatePermission = () => {
      if (!active || !permission) return;
      if (permission.state === "granted") {
        enable();
      } else {
        setEnabled(false);
        setPosition(null);
        setStatus(permission.state === "denied" ? "denied" : "idle");
      }
    };
    navigator.permissions.query({ name: "geolocation" }).then((result) => {
      if (!active) return;
      permission = result;
      permission.addEventListener("change", updatePermission);
      if (permission.state !== "prompt") updatePermission();
    }).catch(() => undefined);
    return () => {
      active = false;
      permission?.removeEventListener("change", updatePermission);
    };
  }, [supported, enable]);

  useEffect(() => {
    if (!enabled || !supported) return;
    let active = true;
    const watcher = navigator.geolocation.watchPosition(
      (value) => {
        if (!active) return;
        setPosition(value);
        setStatus("active");
        setError("");
      },
      (cause) => {
        if (!active) return;
        setPosition(null);
        setEnabled(false);
        setStatus(cause.code === 1 ? "denied" : "error");
        setError(cause.code === 3
          ? "위치를 확인하는 데 시간이 걸려요. 잠시 후 다시 확인해 주세요."
          : "위치를 확인할 수 없어요. 휴대전화의 위치 설정을 확인해 주세요.");
      },
      { enableHighAccuracy: false, maximumAge: 30_000, timeout: 20_000 },
    );
    return () => {
      active = false;
      navigator.geolocation.clearWatch(watcher);
    };
  }, [enabled, attempt, supported]);

  return { status, position, error, enable };
}
