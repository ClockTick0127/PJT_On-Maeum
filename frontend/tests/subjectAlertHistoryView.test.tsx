import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import SubjectAlertHistory from "../src/components/staff/SubjectAlertHistory";
import { getSubjectEvents } from "../src/api/monitoring";
import type { Subject } from "../src/types/monitoring";

vi.mock("../src/api/monitoring", () => ({ getSubjectEvents: vi.fn() }));
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
const subject: Subject = {
  subjectId: "1", name: "박정수", age: 74, address: "", phone: "", version: 1,
  riskLevel: "DANGER", riskScore: 95, updatedAt: "2026-09-26T10:00:00Z", lastActivity: null,
  latestAlert: { alertId: "latest", eventId: null, subjectResponse: { status: "PENDING", answer: null, respondedAt: null } },
  riskTrend: { timezone: "Asia/Seoul", dailyScores: [] },
};
let root: ReturnType<typeof createRoot> | undefined;
let client: QueryClient;
afterEach(async () => {
  if (root) await act(async () => root?.unmount());
  client?.clear();
  document.body.innerHTML = "";
  vi.restoreAllMocks();
  vi.mocked(getSubjectEvents).mockReset();
});

async function render(demo: boolean, target: string) {
  client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const node = document.createElement("div");
  document.body.append(node);
  root = createRoot(node);
  await act(async () => {
    root?.render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[`/staff/subjects/1?alert=${target}`]}><SubjectAlertHistory subject={subject} history={[]} demo={demo} /></MemoryRouter></QueryClientProvider>);
  });
  return node;
}

it("데모에서는 실제 API를 호출하지 않고 선택한 최신 알림을 강조한다", async () => {
  const scroll = vi.fn();
  Object.defineProperty(HTMLElement.prototype, "scrollIntoView", { configurable: true, value: scroll });
  const node = await render(true, "latest");
  expect(getSubjectEvents).not.toHaveBeenCalled();
  expect(node.querySelector("article")?.getAttribute("tabindex")).toBe("-1");
  expect(node.querySelector("article")?.textContent).toContain("응답 대기");
  expect(scroll).toHaveBeenCalled();
  expect(document.activeElement).toBe(node.querySelector("article"));
});

it("이전 페이지에 있는 선택 알림까지 자동 조회하고 감지 내용과 응답을 표시한다", async () => {
  vi.mocked(getSubjectEvents).mockResolvedValueOnce({ events: [], pagination: { hasNext: true, nextCursor: "next" } }).mockResolvedValueOnce({
    events: [{ eventId: "event-old", description: "과거 위험 감지 상세 내용", riskLevel: "DANGER", riskScore: 90, occurredAt: "2026-09-25T10:00:00Z", alert: { alertId: "old", managerStatus: "RESOLVED", subjectResponse: { status: "ANSWERED", answer: "NO", respondedAt: "2026-09-25T10:01:00Z" } } }],
    pagination: { hasNext: false, nextCursor: null },
  });
  const node = await render(false, "old");
  await vi.waitFor(async () => {
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 10)); });
    expect(node.textContent).toContain("과거 위험 감지 상세 내용");
  });
  expect(getSubjectEvents).toHaveBeenCalledTimes(2);
  expect(getSubjectEvents).toHaveBeenLastCalledWith("1", "next", expect.objectContaining({ from: expect.any(String), to: expect.any(String) }));
  const selected = node.querySelector("article[tabindex='-1']");
  expect(selected?.textContent).toContain("괜찮아요");
});
