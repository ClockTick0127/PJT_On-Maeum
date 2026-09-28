import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useEffect } from "react";
import type { ReactNode } from "react";
import { SESSION_EVENT } from "../api/tokenStorage";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 15_000, retry: 1, refetchOnWindowFocus: true },
    mutations: { retry: 0 },
  },
});

export default function QueryProvider({ children }: { children: ReactNode }) {
  useEffect(() => {
    const clearPrivateCache = () => queryClient.clear();
    window.addEventListener(SESSION_EVENT, clearPrivateCache);
    return () => window.removeEventListener(SESSION_EVENT, clearPrivateCache);
  }, []);
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}
