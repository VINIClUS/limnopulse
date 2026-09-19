import { Link, NavLink } from "react-router";
import { useState, type ReactNode } from "react";
import {
  ArrowRight,
  Menu,
  X,
  ShieldCheck,
  ChartNoAxesColumnIncreasing,
  Leaf,
} from "lucide-react";
export function Brand({
  light = false,
  tagline = false,
}: {
  light?: boolean;
  tagline?: boolean;
}) {
  return (
    <Link
      to="/"
      className={`brand ${light ? "brand-light" : ""}`}
      aria-label="LimnoPulse — Início"
    >
      <svg
        width="53"
        height="42"
        viewBox="0 0 58 44"
        fill="none"
        aria-hidden="true"
      >
        <path
          d="M5 17C9 3 15 2 21 15C26 27 33 24 38 10C42 0 49 4 53 16M4 36C10 23 15 21 22 32C28 43 34 41 40 29C46 19 50 25 55 36"
          stroke="currentColor"
          strokeWidth="6"
          strokeLinecap="round"
        />
      </svg>
      <span>
        LimnoPulse{tagline && <small>Água em melhores decisões</small>}
      </span>
    </Link>
  );
}
export function Button({
  children,
  onClick,
  secondary = false,
  className = "",
  type = "button",
  disabled = false,
}: {
  children: ReactNode;
  onClick?: () => void;
  secondary?: boolean;
  className?: string;
  type?: "button" | "submit";
  disabled?: boolean;
}) {
  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={`button ${secondary ? "secondary" : ""} ${className}`}
    >
      {children}
    </button>
  );
}
export function PublicHeader({
  onContact,
  dark = false,
}: {
  onContact: () => void;
  dark?: boolean;
}) {
  const [open, setOpen] = useState(false);
  return (
    <header className={`public-header ${dark ? "header-dark" : ""}`}>
      <Brand light={dark} />
      <button
        className="menu-toggle"
        aria-label={open ? "Fechar menu" : "Abrir menu"}
        aria-expanded={open}
        onClick={() => setOpen(!open)}
      >
        {open ? <X /> : <Menu />}
      </button>
      <nav aria-label="Navegação principal" className={open ? "nav-open" : ""}>
        <NavLink to="/produto" onClick={() => setOpen(false)}>
          Produto
        </NavLink>
        <NavLink to="/como-funciona" onClick={() => setOpen(false)}>
          Como funciona
        </NavLink>
        <NavLink to="/contato" onClick={() => setOpen(false)}>
          Contato
        </NavLink>
      </nav>
      <div className="header-actions">
        <Link className="button secondary" to="/entrar">
          Entrar
        </Link>
        <Button onClick={onContact}>
          Solicitar contato <ArrowRight size={17} />
        </Button>
      </div>
    </header>
  );
}
export function Benefits() {
  return (
    <div className="benefits">
      <span>
        <ChartNoAxesColumnIncreasing /> Mais controle
        <br /> na sua operação
      </span>
      <span>
        <ShieldCheck /> Decisões baseadas
        <br /> em dados reais
      </span>
      <span>
        <Leaf /> Produção mais eficiente
        <br /> e sustentável
      </span>
    </div>
  );
}
export function PhotoBanner({
  onContact,
  title = "Mais controle hoje.",
  subtitle = "Uma aquicultura melhor amanhã.",
}: {
  onContact: () => void;
  title?: string;
  subtitle?: string;
}) {
  return (
    <section className="photo-banner">
      <div>
        <span className="eyebrow">MAIS QUE DADOS, MAIS RESULTADOS</span>
        <h2>
          {title}
          <br />
          <span>{subtitle}</span>
        </h2>
        <p>
          Dados confiáveis. Decisões mais seguras. Um futuro mais produtivo.
        </p>
        <Button onClick={onContact}>
          Conversar com o time <ArrowRight size={18} />
        </Button>
      </div>
      <Brand light />
    </section>
  );
}
export function Footer() {
  return (
    <footer className="footer">
      <div>
        <Brand light />
        <p>Dados de hoje. Aquicultura de amanhã.</p>
      </div>
      <nav aria-label="Navegação do rodapé">
        <Link to="/produto">Produto</Link>
        <Link to="/como-funciona">Como funciona</Link>
        <Link to="/contato">Contato</Link>
      </nav>
      <p>
        © {new Date().getFullYear()} LimnoPulse.
        <br />
        Água em melhores decisões.
      </p>
    </footer>
  );
}
export function ErrorNotice({
  error,
  retry,
}: {
  error: unknown;
  retry?: () => void;
}) {
  return (
    <div className="error-notice" role="alert">
      <span>
        {error instanceof Error
          ? error.message
          : "Não foi possível carregar os dados."}
      </span>
      {retry && (
        <button className="text-button" onClick={retry}>
          Tentar novamente
        </button>
      )}
    </div>
  );
}
export function Empty({ children }: { children: ReactNode }) {
  return <div className="empty-state">{children}</div>;
}
