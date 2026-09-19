import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { vi, it, expect } from "vitest";
import { LeadForm } from "./LeadForm";
it("submits only contact data and shows confirmation after persistence", async () => {
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(JSON.stringify({ lead_id: "1", status: "received" }), {
      status: 201,
    }),
  );
  render(<LeadForm source="/checkout" />);
  const user = userEvent.setup();
  await user.type(screen.getByLabelText(/Nome completo/), "Maria Silva");
  await user.type(screen.getByLabelText(/E-mail/), "maria@example.com");
  await user.click(screen.getByLabelText(/Autorizo/));
  await user.click(screen.getByRole("button", { name: /Enviar interesse/ }));
  expect(await screen.findByText(/Interesse recebido/)).toBeInTheDocument();
  const payload = JSON.parse(fetcher.mock.calls[0][1]?.body as string);
  expect(payload).toEqual({
    name: "Maria Silva",
    email: "maria@example.com",
    phone: null,
    property_name: null,
    consent: true,
    source: "/checkout",
  });
  fetcher.mockRestore();
});
it("preserves fields when submission fails", async () => {
  const fetcher = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 503 }));
  render(<LeadForm source="/" />);
  const user = userEvent.setup();
  await user.type(screen.getByLabelText(/Nome completo/), "Maria Silva");
  await user.type(screen.getByLabelText(/E-mail/), "maria@example.com");
  await user.click(screen.getByLabelText(/Autorizo/));
  await user.click(screen.getByRole("button", { name: /Enviar interesse/ }));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "temporariamente indisponível",
  );
  expect(screen.getByLabelText(/Nome completo/)).toHaveValue("Maria Silva");
  fetcher.mockRestore();
});
