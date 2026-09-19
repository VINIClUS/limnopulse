import { Link } from "react-router";
import {
  ArrowRight,
  ChartNoAxesColumnIncreasing,
  FileText,
  Bell,
  Radio,
  UserRound,
  Cloud,
  Check,
  Leaf,
  Users,
  Gift,
  MessageSquare,
  Monitor,
  Smartphone,
  ShieldCheck,
  CreditCard,
  LockKeyhole,
  FlaskConical,
  CheckCircle2,
} from "lucide-react";
import {
  PublicHeader,
  Footer,
  Button,
  Benefits,
  PhotoBanner,
  Brand,
} from "../components/ui";
import { DemoMonitor, WaterChart, demoSeries } from "../components/WaterChart";
import { LeadForm } from "../components/LeadForm";
type Props = { onContact: () => void };
export function Home({ onContact }: Props) {
  return (
    <>
      <section className="home-hero">
        <PublicHeader dark onContact={onContact} />
        <div className="home-hero-body">
          <div className="hero-copy">
            <span className="eyebrow">TECNOLOGIA A SERVIÇO DA AQUICULTURA</span>
            <h1>
              Entenda sua água.
              <br />
              <span>Antecipe decisões.</span>
            </h1>
            <p>
              Monitoramento inteligente para aquicultura.
              <br />
              Dados, alertas e insights em um único lugar.
            </p>
            <div className="cta-row">
              <Link to="/produto" className="button">
                Conhecer o LimnoPulse <ArrowRight size={19} />
              </Link>
              <Button secondary onClick={onContact}>
                Falar com o time
              </Button>
            </div>
            <div className="hero-checks">
              <span>✓ Acesso em qualquer lugar</span>
              <span>✓ Dados em tempo real</span>
              <span>✓ Mais produtividade</span>
            </div>
          </div>
          <DemoMonitor />
        </div>
      </section>
      <main>
        <section className="feature-grid content-width">
          {[
            {
              icon: ChartNoAxesColumnIncreasing,
              title: "Monitoramento em tempo real",
              text: "Acompanhe as principais variáveis da água, de onde estiver.",
            },
            {
              icon: FileText,
              title: "Histórico completo",
              text: "Tenha dados e histórico para uma gestão mais eficiente.",
            },
            {
              icon: Bell,
              title: "Alertas inteligentes",
              text: "Seja avisado sobre desvios para agir com mais segurança.",
            },
          ].map(({ icon: Icon, title, text }) => (
            <article className="card feature" key={title}>
              <span className="icon-circle">
                <Icon />
              </span>
              <div>
                <h3>{title}</h3>
                <p>{text}</p>
              </div>
            </article>
          ))}
        </section>
        <section className="journey content-width">
          <h2>Da água à decisão</h2>
          <p>
            Tecnologia a serviço de uma produção mais previsível e sustentável.
          </p>
          <div className="journey-steps">
            {[
              {
                icon: Radio,
                title: "Sensores",
                text: (
                  <>
                    Coleta de dados
                    <br />
                    da água em tempo real
                  </>
                ),
              },
              {
                icon: Cloud,
                title: "LimnoPulse",
                text: (
                  <>
                    Processamento e análise
                    <br />
                    na nuvem
                  </>
                ),
              },
              {
                icon: UserRound,
                title: "Produtor",
                text: (
                  <>
                    Informação para agir
                    <br />
                    com mais segurança
                  </>
                ),
              },
            ].map(({ icon: Icon, title, text }, i) => (
              <div key={title}>
                <span className="journey-icon">
                  <Icon />
                </span>
                <h3>{title}</h3>
                <p>{text}</p>
                {i < 2 && (
                  <span className="journey-arrow" aria-hidden="true">
                    ······ <ArrowRight /> ······
                  </span>
                )}
              </div>
            ))}
          </div>
        </section>
        <section className="contact-strip content-width">
          <div>
            <span className="eyebrow">SUA OPERAÇÃO, MAIS CONECTADA</span>
            <h2>
              O próximo passo começa
              <br />
              com uma conversa.
            </h2>
            <p>Conheça uma nova forma de cuidar da sua água.</p>
          </div>
          <Button onClick={onContact}>
            Quero conhecer <ArrowRight size={18} />
          </Button>
        </section>
      </main>
      <Footer onContact={onContact} />
    </>
  );
}
export function Product({ onContact }: Props) {
  return (
    <>
      <PublicHeader onContact={onContact} />
      <main>
        <section className="product-hero">
          <div className="content-width product-hero-inner">
            <div>
              <span className="eyebrow">TECNOLOGIA PARA AQUICULTURA</span>
              <h1>
                Veja sua produção
                <br />
                <span>como um sistema.</span>
              </h1>
              <p>
                O LimnoPulse conecta dados, contexto e operação
                <br />
                para ajudar você a tomar melhores decisões
                <br />
                na aquicultura.
              </p>
              <div className="cta-row">
                <Button onClick={onContact}>
                  Solicitar contato <ArrowRight size={18} />
                </Button>
                <a className="button secondary" href="#como-funciona">
                  Como funciona
                </a>
              </div>
              <Benefits />
            </div>
            <DemoMonitor small />
          </div>
        </section>
        <section id="como-funciona" className="how-it-works content-width">
          <span className="eyebrow">DO DADO À DECISÃO</span>
          <h2>Como funciona</h2>
          <p>
            Do monitoramento à ação, em um ciclo contínuo de informação e
            resultados.
          </p>
          <div className="process-list">
            {[
              {
                icon: FlaskConical,
                title: "Coleta de dados",
                text: "Sensores monitoram continuamente oxigênio, pH, temperatura e outras variáveis.",
                visual: (
                  <div className="sample-values">
                    <span>
                      O₂
                      <strong>
                        6,4 <small>mg/L</small>
                      </strong>
                    </span>
                    <span>
                      pH<strong>7,3</strong>
                    </span>
                    <span>
                      Temp.
                      <strong>
                        28,1 <small>°C</small>
                      </strong>
                    </span>
                  </div>
                ),
                aside: "Dados confiáveis, em tempo real.",
              },
              {
                icon: ChartNoAxesColumnIncreasing,
                title: "Contexto",
                text: "Dados isolados dizem pouco. O histórico mostra tendências e revela oportunidades.",
                visual: <WaterChart data={demoSeries} compact />,
                aside: "Histórico que transforma dados em visão.",
              },
              {
                icon: Bell,
                title: "Alertas",
                text: "Você recebe alertas quando alguma variável sai da faixa definida.",
                visual: (
                  <div className="sample-alert">
                    <Bell />
                    <div>
                      <strong>Oxigênio abaixo da faixa</strong>
                      <span>Viveiro 03 — 3,8 mg/L</span>
                      <small>Exemplo de alerta</small>
                    </div>
                  </div>
                ),
                aside: "Mais rapidez para agir.",
              },
              {
                icon: CheckCircle2,
                title: "Decisão",
                text: "Com informação clara, você antecipa problemas e toma decisões com segurança.",
                visual: (
                  <div className="sample-tasks">
                    <span>✓ Ajustar aeração</span>
                    <span>✓ Verificar alimentação</span>
                    <span>✓ Acompanhar tendência</span>
                  </div>
                ),
                aside: "Da informação a resultados no campo.",
              },
            ].map(({ icon: Icon, title, text, visual, aside }, i) => (
              <article className="card process-row" key={title}>
                <span className="step-number">0{i + 1}</span>
                <span className="icon-circle">
                  <Icon />
                </span>
                <div>
                  <h3>{title}</h3>
                  <p>{text}</p>
                </div>
                <div className="process-visual">{visual}</div>
                <p className="process-aside">{aside}</p>
              </article>
            ))}
          </div>
        </section>
        <section className="platform">
          <div className="content-width platform-inner">
            <div>
              <span className="eyebrow">PLATAFORMA COMPLETA</span>
              <h2>Tudo em um só lugar</h2>
              <p>
                Uma plataforma para simplificar sua rotina
                <br />e potencializar sua produção.
              </p>
            </div>
            {[
              {
                icon: Monitor,
                title: "Dashboard",
                text: "Visão clara dos indicadores da sua operação.",
              },
              {
                icon: FileText,
                title: "Histórico",
                text: "Dados para análise e tomada de decisão.",
              },
              {
                icon: Smartphone,
                title: "Multi-dispositivos",
                text: "Acesse no computador, tablet ou celular.",
              },
            ].map(({ icon: Icon, title, text }) => (
              <article className="card" key={title}>
                <Icon />
                <h3>{title}</h3>
                <p>{text}</p>
              </article>
            ))}
          </div>
        </section>
        <PhotoBanner
          onContact={onContact}
          title="Tecnologia que trabalha"
          subtitle="junto com você, em cada ciclo."
        />
      </main>
    </>
  );
}
const plans = [
  {
    name: "Essencial",
    icon: Leaf,
    description: "Para começar a monitorar",
    price: "149,90",
    old: "199,90",
    annual: "1.798,80",
    saving: "600",
    features: [
      "1 propriedade",
      "2 viveiros",
      "4 dispositivos",
      "Dashboard",
      "Histórico básico",
      "Alertas por e-mail",
    ],
  },
  {
    name: "Pro",
    icon: ChartNoAxesColumnIncreasing,
    description: "Para operações em crescimento",
    price: "299,90",
    old: "399,90",
    annual: "3.598,80",
    saving: "1.200",
    features: [
      "1 propriedade",
      "10 viveiros",
      "20 dispositivos",
      "Dashboard completo",
      "Histórico completo",
      "Alertas avançados",
      "Relatórios",
      "Até 5 usuários",
    ],
  },
  {
    name: "Operação",
    icon: Users,
    description: "Para operações maiores",
    price: "599,90",
    old: "799,90",
    annual: "7.198,80",
    saving: "2.400",
    features: [
      "3 propriedades",
      "30 viveiros",
      "60 dispositivos",
      "Dashboard completo",
      "Histórico completo",
      "Alertas avançados",
      "Relatórios",
      "Equipe e permissões",
      "API / integrações",
      "Suporte prioritário",
    ],
  },
];
export function Plans({ onContact }: Props) {
  return (
    <div className="plans-page">
      <PublicHeader onContact={onContact} />
      <main>
        <section className="plans-intro content-width">
          <span className="eyebrow">
            TECNOLOGIA PARA AQUICULTURA SUSTENTÁVEL
          </span>
          <h1>
            Planos para cada momento
            <br />
            da <span>sua operação</span>
          </h1>
          <p>Conheça as opções para transformar dados em resultados.</p>
          <div className="launch-note">
            <Gift />
            <span>
              <strong>Prévia de planos</strong> — Valores demonstrativos.
              Contratação e cobrança indisponíveis.
            </span>
          </div>
        </section>
        <section className="plan-grid content-width">
          {plans.map(
            (
              {
                name,
                icon: Icon,
                description,
                price,
                old,
                annual,
                saving,
                features,
              },
              i,
            ) => (
              <article
                key={name}
                className={`card plan-card ${i === 1 ? "featured-plan" : ""}`}
              >
                {i === 1 && <div className="plan-ribbon">★ Mais escolhido</div>}
                <span className="icon-circle">
                  <Icon />
                </span>
                <h2>{name}</h2>
                <p>{description}</p>
                <del>R$ {old}/mês</del>
                <small className="installments">12x de</small>
                <strong className="plan-price">R$ {price}</strong>
                <p>
                  Total anual
                  <br />
                  <strong>R$ {annual}</strong> no plano anual
                </p>
                <div className="savings">Economize R$ {saving}</div>
                <ul className="check-list">
                  {features.map((f) => (
                    <li key={f}>
                      <Check />
                      {f}
                    </li>
                  ))}
                </ul>
                <Link
                  className={`button ${i !== 1 ? "secondary" : ""}`}
                  to={`/checkout?plano=${encodeURIComponent(name)}`}
                >
                  Registrar interesse <ArrowRight size={16} />
                </Link>
              </article>
            ),
          )}
        </section>
        <section className="faq content-width">
          <div>
            <span className="eyebrow">PERGUNTAS FREQUENTES</span>
            <h2>Tire suas dúvidas</h2>
            <p>
              Respostas rápidas para você
              <br />
              conhecer o LimnoPulse.
            </p>
            <button className="faq-contact" onClick={onContact}>
              <MessageSquare />
              <span>
                <strong>Ainda tem dúvidas?</strong>
                <br />
                Fale com nosso time <ArrowRight size={14} />
              </span>
            </button>
          </div>
          <div>
            {[
              [
                "Como funciona o pagamento?",
                "O pagamento está desativado. Você pode registrar interesse, sem cobrança ou compromisso de contratação.",
              ],
              [
                "O hardware está incluso?",
                "O hardware é tratado separadamente. Nosso time ajuda a entender a necessidade da sua operação.",
              ],
              [
                "Posso mudar de plano depois?",
                "As condições comerciais serão apresentadas quando a contratação estiver disponível.",
              ],
              [
                "Tem contrato de fidelidade?",
                "Não há contratação nesta página. Registre seu interesse para conversar sobre as condições.",
              ],
              [
                "Em quanto tempo posso começar?",
                "Deixe seus dados para que o time entenda sua operação e converse sobre os próximos passos.",
              ],
            ].map(([q, a]) => (
              <details key={q}>
                <summary>{q}</summary>
                <p>{a}</p>
              </details>
            ))}
          </div>
        </section>
        <PhotoBanner
          onContact={onContact}
          title="Comece agora e leve mais"
          subtitle="previsibilidade para sua produção."
        />
      </main>
    </div>
  );
}
export function Checkout({ onContact }: Props) {
  const selected = new URLSearchParams(location.search).get("plano");
  const plan = plans.find((p) => p.name === selected) || plans[1];
  return (
    <>
      <PublicHeader onContact={onContact} />
      <main className="checkout content-width">
        <Link className="back-link" to="/planos">
          ← Voltar para os planos
        </Link>
        <div className="checkout-grid">
          <div>
            <h1>Checkout</h1>
            <p className="page-subtitle">
              Conheça a solução para monitorar sua água com mais
              <br />
              inteligência e tranquilidade.
            </p>
            <section className="card checkout-form">
              <div className="section-heading">
                <span className="step-number">1</span>
                <div>
                  <h2>Seus dados</h2>
                  <p>Registre seu interesse no LimnoPulse. Sem cobrança.</p>
                </div>
              </div>
              <LeadForm source={`/checkout?plano=${plan.name}`}>
                <fieldset disabled className="payment-fields">
                  <label>
                    CPF / CNPJ
                    <input placeholder="Não solicitado" />
                  </label>
                  <div className="section-heading">
                    <span className="step-number">2</span>
                    <div>
                      <h2>Pagamento</h2>
                      <p>
                        Pagamento desativado. Nenhum dado financeiro é coletado.
                      </p>
                    </div>
                  </div>
                  <div className="card payment-box">
                    <strong>
                      <CreditCard /> Cartão de crédito <span>Indisponível</span>
                    </strong>
                    <label>
                      Número do cartão
                      <input
                        placeholder="0000 0000 0000 0000"
                        autoComplete="off"
                      />
                    </label>
                    <div className="form-grid">
                      <label>
                        Validade
                        <input placeholder="MM/AA" />
                      </label>
                      <label>
                        CVV
                        <input placeholder="•••" />
                      </label>
                    </div>
                    <label>
                      Parcelamento
                      <select defaultValue="disabled">
                        <option value="disabled">
                          Contratação indisponível
                        </option>
                      </select>
                    </label>
                  </div>
                </fieldset>
              </LeadForm>
            </section>
          </div>
          <aside className="card order-summary">
            <h2>
              <FileText /> Resumo do interesse
            </h2>
            <hr />
            <h2>Plano {plan.name}</h2>
            <p>Tecnologia para uma produção mais previsível.</p>
            <span className="badge">Prévia demonstrativa</span>
            <small className="installments">12x de</small>
            <strong className="plan-price">R$ {plan.price}</strong>
            <p>
              Total anual
              <br />
              <strong>R$ {plan.annual}</strong>
            </p>
            <div className="savings">Nenhuma cobrança será realizada</div>
            <hr />
            <h3>Referência do plano</h3>
            <ul className="check-list">
              {plan.features.map((f) => (
                <li key={f}>
                  <Check />
                  {f}
                </li>
              ))}
            </ul>
            <hr />
            <div className="summary-note">
              <ShieldCheck />
              <div>
                <strong>Seus dados protegidos</strong>
                <p>
                  Apenas os dados de contato são enviados para registrar seu
                  interesse.
                </p>
              </div>
            </div>
            <hr />
            <button className="text-button summary-note" onClick={onContact}>
              <MessageSquare />
              <span>Dúvidas? Fale conosco</span>
            </button>
            <p className="privacy-note">
              <LockKeyhole size={14} /> Pagamento desativado
            </p>
          </aside>
        </div>
      </main>
      <section className="checkout-footer">
        <h2>
          Mais controle hoje.
          <br />
          <span>Uma aquicultura melhor amanhã.</span>
        </h2>
        <Brand light />
      </section>
    </>
  );
}
