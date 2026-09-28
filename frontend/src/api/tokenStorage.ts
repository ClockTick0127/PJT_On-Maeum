import type { AuthTokens } from "../types/auth";

const ACCESS_TOKEN_KEY = "onmaeum.accessToken";
const REFRESH_TOKEN_KEY = "onmaeum.refreshToken";
const SESSION_ROLE_KEY = "onmaeum.sessionRole";
export const SESSION_EVENT = "onmaeum:session-changed";
export type SessionRole = "staff" | "user";
let generation = 0;

function hasPersistentSession() {
  return Boolean(localStorage.getItem(ACCESS_TOKEN_KEY));
}

function getStoredToken(key: string) {
  return localStorage.getItem(key) ?? sessionStorage.getItem(key);
}

function notifySessionChanged() {
  window.dispatchEvent(new Event(SESSION_EVENT));
}

export function getAccessToken() {
  return getStoredToken(ACCESS_TOKEN_KEY);
}

export function getRefreshToken() {
  return getStoredToken(REFRESH_TOKEN_KEY);
}

export function getSessionRole(): SessionRole | null {
  const role = getStoredToken(SESSION_ROLE_KEY);
  return role === "staff" || role === "user" ? role : null;
}

export function hasSession() {
  return Boolean(getAccessToken());
}

export function getSessionGeneration() {
  return generation;
}

export function saveSession(
  tokens: AuthTokens,
  persistent = hasPersistentSession(),
  role = getSessionRole(),
) {
  const wasActive = hasSession();
  const previousRole = getSessionRole();
  const storage = persistent ? localStorage : sessionStorage;
  const previousStorage = persistent ? sessionStorage : localStorage;
  previousStorage.removeItem(ACCESS_TOKEN_KEY);
  previousStorage.removeItem(REFRESH_TOKEN_KEY);
  previousStorage.removeItem(SESSION_ROLE_KEY);
  storage.setItem(ACCESS_TOKEN_KEY, tokens.accessToken);
  storage.setItem(REFRESH_TOKEN_KEY, tokens.refreshToken);
  if (role) storage.setItem(SESSION_ROLE_KEY, role);
  else storage.removeItem(SESSION_ROLE_KEY);
  if (!wasActive || previousRole !== role) notifySessionChanged();
}

export function clearSession() {
  const wasActive = hasSession();
  generation += 1;
  sessionStorage.removeItem(ACCESS_TOKEN_KEY);
  sessionStorage.removeItem(REFRESH_TOKEN_KEY);
  sessionStorage.removeItem(SESSION_ROLE_KEY);
  localStorage.removeItem(ACCESS_TOKEN_KEY);
  localStorage.removeItem(REFRESH_TOKEN_KEY);
  localStorage.removeItem(SESSION_ROLE_KEY);
  if (wasActive) notifySessionChanged();
}
