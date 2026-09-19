import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router";
import { beforeEach, expect, it, vi } from "vitest";

const session = vi.hoisted(() => ({
  user: { id: "user-a", email: "a@example.com" },
  exit: vi.fn(),
}));
vi.mock("../lib/api", () => ({
  api: vi.fn(),
  post: vi.fn(),
}));
vi.mock("../lib/session", () => ({ useSession: () => session }));
import { Onboarding } from "./Onboarding";

const draft = (name: string) => ({
  step: 0,
  name,
  city: "",
  ponds: [{ name: "" }],
  deviceName: "",
});

beforeEach(() => {
  sessionStorage.clear();
  session.user = { id: "user-a", email: "a@example.com" };
  sessionStorage.setItem(
    "limnopulse:onboarding:user-a",
    JSON.stringify(draft("Draft A")),
  );
  sessionStorage.setItem(
    "limnopulse:onboarding:user-b",
    JSON.stringify(draft("Draft B")),
  );
});

it("loads the new user's onboarding draft when the session identity changes", () => {
  const queryClient = new QueryClient();
  const view = render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <Onboarding />
      </MemoryRouter>
    </QueryClientProvider>,
  );

  expect(screen.getByLabelText("Nome da propriedade")).toHaveValue("Draft A");

  session.user = { id: "user-b", email: "b@example.com" };
  view.rerender(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <Onboarding />
      </MemoryRouter>
    </QueryClientProvider>,
  );

  expect(screen.getByLabelText("Nome da propriedade")).toHaveValue("Draft B");
});
