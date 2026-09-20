# Verificação visual — LimnoPulse

Capturas finais de 2026-09-19. [Abrir relatório interativo](index.html).

## Método

- Desktop: 1448×1086 para login, onboarding e dashboard; 1086×1448 para início, produto, planos e checkout.
- Mobile: 390×844, com imagem do viewport e da página inteira.
- Playwright Chromium, locale pt-BR, timezone America/Sao_Paulo, relógio em 19/09/2026 às 20:30.
- Inter servida localmente, fontes e fundos carregados, animações e cursor desativados.
- HTTP controlado somente na suíte visual. Integração real foi verificada separadamente.
- Diferença calculada por pixelmatch (limiar 0,15, excluindo antialiasing); imagens comparadas nas dimensões exatas das referências.
- As diferenças percentuais incluem texto intencionalmente alterado e fotos geradas; não indicam taxa de erro funcional ou equivalência pixel a pixel.

## Resultado por tela

| Tela | Dimensões | Pixels diferentes | Evidência |
| --- | --- | --- | --- |
| dashboard | 1448×1086 | 15.12% | [Lado a lado](comparisons/dashboard-side-by-side.png) · [Overlay](comparisons/dashboard-overlay.png) · [Diff](comparisons/dashboard-diff.png) |
| onboarding | 1448×1086 | 11.25% | [Lado a lado](comparisons/onboarding-side-by-side.png) · [Overlay](comparisons/onboarding-overlay.png) · [Diff](comparisons/onboarding-diff.png) |
| login | 1448×1086 | 19.07% | [Lado a lado](comparisons/login-side-by-side.png) · [Overlay](comparisons/login-overlay.png) · [Diff](comparisons/login-diff.png) |
| checkout | 1086×1448 | 15.43% | [Lado a lado](comparisons/checkout-side-by-side.png) · [Overlay](comparisons/checkout-overlay.png) · [Diff](comparisons/checkout-diff.png) |
| planos | 1086×1448 | 19.24% | [Lado a lado](comparisons/planos-side-by-side.png) · [Overlay](comparisons/planos-overlay.png) · [Diff](comparisons/planos-diff.png) |
| produto | 1086×1448 | 22.52% | [Lado a lado](comparisons/produto-side-by-side.png) · [Overlay](comparisons/produto-overlay.png) · [Diff](comparisons/produto-diff.png) |
| inicio | 1086×1448 | 23.95% | [Lado a lado](comparisons/inicio-side-by-side.png) · [Overlay](comparisons/inicio-overlay.png) · [Diff](comparisons/inicio-diff.png) |

### dashboard

Dados controlados apenas nesta captura. Quatro viveiros correspondem às quatro linhas da fixture. Conexão desconhecida, alertas derivados de eventos e seletor de viveiro/período. Navegação limitada ao escopo; detalhes de dispositivos/alertas abaixo do painel.

[Desktop](screenshots/dashboard-desktop-full.png) · [Mobile](screenshots/dashboard-mobile-full.png)

### onboarding

Cidade livre e opcional em vez de lista predefinida. Foto reconstruída; estados adicionais do fluxo são funcionais. Dados preenchidos só no teste.

[Desktop](screenshots/onboarding-desktop-full.png) · [Mobile](screenshots/onboarding-mobile-full.png)

### login

Link comercial substituído por contato. Fotografia reconstruída; cartões explicitamente ilustrativos. Sem cadastro automático. Banner local oculto somente na captura visual.

[Desktop](screenshots/login-desktop-full.png) · [Mobile](screenshots/login-mobile-full.png)

### checkout

Sem cobrança ou criação de conta. CPF e cartão desativados. Campos de consentimento e contato alteram a altura e a disposição do formulário.

[Desktop](screenshots/checkout-desktop-full.png) · [Mobile](screenshots/checkout-mobile-full.png)

### planos

Rota oculta, noindex, preços apenas demonstrativos. CTA registra interesse. Preço anterior de Operação corrigido para coerência aritmética com a economia ilustrada.

[Desktop](screenshots/planos-desktop-full.png) · [Mobile](screenshots/planos-mobile-full.png)

### produto

CTAs comerciais substituídos por contato; histórico apresentado sem prometer página extra de relatórios. Mantidas as quatro etapas e cartões de benefícios.

[Desktop](screenshots/produto-desktop-full.png) · [Mobile](screenshots/produto-mobile-full.png)

### inicio

CTAs e faixa comercial substituídos por contato. Cartão ilustrativo identificado como demonstração. Fotografia, marca vetorial e ícones reconstruídos.

[Desktop](screenshots/inicio-desktop-full.png) · [Mobile](screenshots/inicio-mobile-full.png)

## Inspeção e correções

- Ajustados largura e alinhamento do cartão demonstrativo da home, altura dos cartões de benefícios, espaçamento das listas de planos, FAQ e gráfico do dashboard.
- Login e onboarding mantêm estrutura e alinhamento próximos às referências; fotos, marca em SVG e Lucide são reconstruções, não assets originais.
- O dashboard acrescenta seletor de viveiro e estados reais. Isso desloca verticalmente o gráfico em relação à referência.
- Todas as sete telas passaram na checagem automatizada de ausência de overflow horizontal em mobile. Formulários, modal nativo, menu móvel e tabelas são acessíveis por teclado.
- Não foi feita uma certificação formal de acessibilidade; não há afirmação de reprodução pixel-perfect.

## Validação funcional

- 354 testes Python passaram com integrações locais habilitadas; 5 testes existentes de notificações permaneceram ignorados por seu gate de execução.
- 14 testes frontend passaram (Cognito adapter/formulários, recuperação, cache entre abas, access token, refresh, leads).
- 21 testes Playwright visuais/estados passaram.
- 2 testes Playwright com API local real passaram (onboarding sem duplicação ao voltar, dashboard, saída, leads e campos financeiros desativados).
- 6 smoke tests Playwright sobre o build de produção passaram; dev auth permanece bloqueado mesmo com flag de desenvolvimento no build.
- TypeScript e build passaram. Dependências de produção: npm audit sem vulnerabilidades conhecidas no momento da verificação.
- Integração real: DynamoDB Local, Redis e InfluxDB; estatísticas verificadas com 1.800 amostras de dois dispositivos, inclusive contagem acima do limite de leituras antigas.
- CSV administrativo executado com sucesso contra DynamoDB Local; paginação multi-mês e neutralização de fórmulas cobertas por teste.
- Cognito remoto não exercitado por falta de configuração/conta de teste; SDK e fluxo de UI cobertos separadamente.
- Avisos preexistentes/ambientais: Starlette sobre httpx TestClient e conflito NO_COLOR/FORCE_COLOR do runner.

## Revisão independente

Uma revisão independente apontou quatro problemas, todos reproduzidos em testes e corrigidos: troca de identidade entre abas, perda de sessão em falha temporária de refresh, alertas indisponíveis parecendo saudáveis e remoção de settings nulos não relacionados ao editar cidade. Nenhum achado foi adiado.
