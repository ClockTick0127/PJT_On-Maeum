import { useState } from "react";
import type { ReactNode } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { http, HttpResponse } from "msw";
import { MemoryRouter } from "react-router-dom";
import UserPage from "../pages/UserPage";

function StoryProviders({ children }: { children: ReactNode }) {
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: { retry: false },
          mutations: { retry: false },
        },
      }),
  );

  return (
    <MemoryRouter initialEntries={["/user"]}>
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    </MemoryRouter>
  );
}

const meta = {
  title: "페이지/대상자 홈",
  component: UserPage,
  decorators: [
    (Story) => (
      <StoryProviders>
        <Story />
      </StoryProviders>
    ),
  ],
  parameters: {
    layout: "fullscreen",
    msw: {
      handlers: [
        http.get("/api/monitoring/my-dashboard", () =>
          HttpResponse.json({
            subjectId: "storybook-user",
            name: "박정수",
            awayMode: {
              enabled: false,
              scheduled: false,
              startedAt: null,
              until: null,
            },
            manager: {
              name: "이돌봄",
              phone: "02-1234-5678",
            },
          }),
        ),
        http.put(
          "/api/monitoring/my-dashboard/away-mode",
          () => new HttpResponse(null, { status: 204 }),
        ),
      ],
    },
  },
} satisfies Meta<typeof UserPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const 기본: Story = {};
