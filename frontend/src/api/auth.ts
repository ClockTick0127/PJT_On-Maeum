import type { AuthTokens, MeResponse } from "../types/auth";
import { publicApi, request } from "./client";

export async function login(email: string, password: string) {
  const { data } = await publicApi.post<AuthTokens>("/auth/login", { email, password });
  return data;
}

export function getMe() {
  return request<MeResponse>({ url: "/auth/me" });
}
