import axios, { AxiosError } from "axios";
import type { AxiosRequestConfig, InternalAxiosRequestConfig } from "axios";
import type { AuthTokens } from "../types/auth";
import {
  clearSession,
  getAccessToken,
  getRefreshToken,
  getSessionGeneration,
  saveSession,
} from "./tokenStorage";

type RetryConfig = InternalAxiosRequestConfig & { _retry?: boolean };
type ErrorBody = { message?: string; detail?: string };

export const publicApi = axios.create({ baseURL: "/api", withCredentials: true });
export const authApi = axios.create({ baseURL: "/api", withCredentials: true });

let refreshRequest: Promise<AuthTokens> | null = null;

authApi.interceptors.request.use((config) => {
  const token = getAccessToken();
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

authApi.interceptors.response.use(undefined, async (error: AxiosError) => {
  const config = error.config as RetryConfig | undefined;
  const refreshToken = getRefreshToken();
  if (error.response?.status !== 401 || !config || config._retry || !refreshToken)
    return Promise.reject(error);
  config._retry = true;
  if (!refreshRequest) {
    const generation = getSessionGeneration();
    refreshRequest = publicApi
      .post<AuthTokens>("/auth/refresh", { refreshToken })
      .then(({ data }) => {
        if (generation !== getSessionGeneration()) throw new Error("로그인이 종료되었습니다.");
        saveSession(data);
        return data;
      })
      .finally(() => {
        refreshRequest = null;
      });
  }
  try {
    const tokens = await refreshRequest;
    config.headers.Authorization = `Bearer ${tokens.accessToken}`;
    return authApi(config);
  } catch (refreshError) {
    if (getRefreshToken() === refreshToken) clearSession();
    return Promise.reject(refreshError);
  }
});

export function getApiErrorMessage(error: unknown) {
  if (!axios.isAxiosError<ErrorBody>(error))
    return error instanceof Error ? error.message : "잠시 후 다시 시도해 주세요.";
  const body = error.response?.data;
  if (typeof body?.message === "string") return body.message;
  if (typeof body?.detail === "string") return body.detail;
  if (error.response?.status === 401) return "로그인이 필요하거나 로그인 시간이 만료되었습니다.";
  if (error.response?.status === 403) return "이 화면에 접근할 권한이 없습니다.";
  return error.response
    ? `요청을 처리하지 못했습니다. (${error.response.status})`
    : "서버에 연결할 수 없습니다.";
}

export async function request<T>(config: AxiosRequestConfig) {
  const response = await authApi.request<T>(config);
  return response.data;
}
