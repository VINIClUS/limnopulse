import { useEffect, useState } from "react";
import { Routes, Route, useLocation, Link } from "react-router";
import { Home, Product, Plans, Checkout } from "./pages/Public";
import { Login } from "./pages/Login";
import { Onboarding } from "./pages/Onboarding";
import { Dashboard } from "./pages/Dashboard";
import { Protected } from "./lib/session";
import { ContactModal } from "./components/LeadForm";
export function App() {
  const location = useLocation(),
    [contact, setContact] = useState(false);
  const openContact = () => setContact(true);
  useEffect(() => {
    const hidden = [
      "/planos",
      "/checkout",
      "/entrar",
      "/onboarding",
      "/app",
    ].includes(location.pathname);
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
      "/planos": "Planos",
      "/checkout": "Registre seu interesse",
      "/entrar": "Entrar",
      "/onboarding": "Configure sua operação",
      "/app": "Visão geral",
    };
    document.title = `${titles[location.pathname] || "Página não encontrada"} | LimnoPulse`;
    setContact(false);
    if (location.hash) {
      requestAnimationFrame(() =>
        document.querySelector(location.hash)?.scrollIntoView(),
      );
    } else window.scrollTo(0, 0);
  }, [location.pathname, location.hash]);
  return (
    <>
      <Routes>
        <Route path="/" element={<Home onContact={openContact} />} />
        <Route path="/produto" element={<Product onContact={openContact} />} />
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
