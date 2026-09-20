import { useEffect, useState } from "react";
import { Routes, Route, useLocation, Link, Navigate } from "react-router";
import {
  Home,
  Product,
  HowItWorks,
  Contact,
  Plans,
  Checkout,
} from "./pages/Public";
import { Login } from "./pages/Login";
import { Onboarding } from "./pages/Onboarding";
import { Dashboard } from "./pages/Dashboard";
import { AlertEventDetail } from "./pages/AlertEventDetail";
import { Protected } from "./lib/session";
import { ContactModal } from "./components/LeadForm";
export function App() {
  const location = useLocation(),
    [contact, setContact] = useState(false);
  const openContact = () => setContact(true);
  useEffect(() => {
    const hidden =
      ["/planos", "/checkout", "/entrar", "/onboarding", "/app"].includes(
        location.pathname,
      ) || location.pathname.startsWith("/tenants/");
    let meta = document.querySelector<HTMLMetaElement>('meta[name="robots"]');
    if (!meta) {
      meta = document.createElement("meta");
      meta.name = "robots";
      document.head.append(meta);
    }
    meta.content = hidden ? "noindex, nofollow" : "index, follow";
    const titles: Record<string, string> = {
      "/": "Água em melhores decisões",
      "/produto": "Produto",
      "/como-funciona": "Como funciona",
      "/contato": "Contato",
      "/planos": "Planos",
      "/checkout": "Registre seu interesse",
      "/entrar": "Entrar",
      "/onboarding": "Configure sua operação",
      "/app": "Visão geral",
    };
    document.title = `${titles[location.pathname] || "Página não encontrada"} | LimnoPulse`;
    setContact(false);
    if (location.hash) {
      requestAnimationFrame(() => {
        let id: string;
        try {
          id = decodeURIComponent(location.hash.slice(1));
        } catch {
          return;
        }
        document.getElementById(id)?.scrollIntoView();
      });
    } else window.scrollTo(0, 0);
  }, [location.pathname, location.hash]);
  return (
    <>
      <Routes>
        <Route path="/" element={<Home onContact={openContact} />} />
        <Route
          path="/produto"
          element={
            location.hash === "#como-funciona" ? (
              <Navigate to="/como-funciona" replace />
            ) : (
              <Product onContact={openContact} />
            )
          }
        />
        <Route
          path="/como-funciona"
          element={<HowItWorks onContact={openContact} />}
        />
        <Route path="/contato" element={<Contact onContact={openContact} />} />
        <Route path="/planos" element={<Plans onContact={openContact} />} />
        <Route
          path="/checkout"
          element={<Checkout onContact={openContact} />}
        />
        <Route path="/entrar" element={<Login onContact={openContact} />} />
        <Route
          path="/onboarding"
          element={
            <Protected>
              <Onboarding />
            </Protected>
          }
        />
        <Route
          path="/app"
          element={
            <Protected>
              <Dashboard onContact={openContact} />
            </Protected>
          }
        />
        <Route
          path="/tenants/:tenantId/alert-events/:eventId"
          element={
            <Protected>
              <AlertEventDetail />
            </Protected>
          }
        />
        <Route
          path="*"
          element={
            <main className="empty-state">
              <h1>Página não encontrada</h1>
              <Link to="/">Voltar ao início</Link>
            </main>
          }
        />
      </Routes>
      <ContactModal
        open={contact}
        onClose={() => setContact(false)}
        source={location.pathname}
      />
    </>
  );
}
