import { useQuery } from "@tanstack/react-query";
import { getMe } from "../../api/auth";

export function useMeQuery() {
  return useQuery({
    queryKey: ["auth", "me"],
    queryFn: getMe,
    staleTime: 5 * 60_000,
    gcTime: 0,
  });
}
