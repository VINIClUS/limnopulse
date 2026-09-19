# LimnoPulse frontend

React 19, TypeScript, Vite, React Router, TanStack Query, Recharts, Lucide e Inter local. As telas estão integradas aos contratos `/v1` da API Python. Não há cobrança, criação automática de conta, envio de e-mail, CRM ou deploy nesta entrega.

## Executar localmente

Na raiz do repositório (use `rtk` antes dos comandos se disponível):

```sh
docker compose up -d redis dynamodb-local influxdb
APP_ENV=local AUTH_MODE=dev DYNAMODB_ENDPOINT_URL=http://127.0.0.1:8001 uv run python scripts/dev/init_dynamodb.py
APP_ENV=local AUTH_MODE=dev DYNAMODB_ENDPOINT_URL=http://127.0.0.1:8001 uv run uvicorn limnopulse_api.main:app --host 127.0.0.1 --port 8000
```

Em outro terminal:

```sh
cd frontend
npm ci
VITE_DEV_AUTH=true VITE_DEV_USER_SUB=local-user npm run dev
```

Abra http://127.0.0.1:3000. No modo local, qualquer e-mail/senha não vazia entra como o `VITE_DEV_USER_SUB` configurado; isso é um simulador de identidade local, não uma autenticação real. Cadastre uma propriedade no onboarding. O proxy encaminha `/v1` para `127.0.0.1:8000` (alterável por `API_PROXY_TARGET`). Os dados iniciais estão vazios; o dashboard não insere telemetria fictícia.

O modo de desenvolvimento exige servidor Vite dev ou modo `test`, `VITE_DEV_AUTH=true` e hostname de loopback. Um build de produção ignora essa opção. A API também exige `APP_ENV=local|test` para `AUTH_MODE=dev`.

## Cognito

Copie `.env.example` para `.env.local` e configure `VITE_COGNITO_USER_POOL_ID` e `VITE_COGNITO_CLIENT_ID` com os IDs públicos existentes. Remova `VITE_DEV_AUTH`. Nunca coloque segredo de cliente ou credenciais AWS em variáveis `VITE_*`.

O cliente existente em `infra/opentofu/cognito.tf` já permite `ALLOW_USER_SRP_AUTH` e refresh, sem secret. Configure a API com `AUTH_MODE=cognito`, pool, client e issuer correspondentes. As contas devem existir previamente no Cognito.

- Login próprio via Amplify Auth/SRP. API recebe **access token**, nunca ID token.
- `fetchAuthSession` renova tokens expirados; após 401, uma renovação forçada e uma repetição são permitidas.
- Falhas temporárias de renovação preservam os tokens; autenticação definitivamente inválida encerra a sessão.
- “Manter conectado” usa `localStorage`; desmarcado usa `sessionStorage`. Alterações de identidade em outras abas cancelam consultas e limpam o cache antes de reconciliar a sessão.
- Sair limpa tokens, cache e rascunhos de onboarding do navegador. Identificadores de entidades salvos no servidor permanecem.
- Recuperação solicita código e confirma nova senha. Também há confirmação de senha temporária e MFA por código. Etapas de configuração de MFA não previstas indicam contato com o administrador.

A validação desta entrega cobre o adaptador e os fluxos de formulário com o SDK substituído em testes unitários. Não houve login/recuperação contra o Cognito remoto: não foram fornecidos IDs e credenciais de uma conta de teste.

## Rotas

| Rota | Acesso |
| --- | --- |
| `/` | Início público |
| `/produto` | Apresentação do produto |
| `/como-funciona` | Etapas do monitoramento à decisão |
| `/contato` | Formulário público de registro de interesse |
| `/planos` | URL direta, sem link na navegação, noindex |
| `/checkout` | URL direta, apenas registro de interesse, noindex |
| `/entrar` | Login e recuperação |
| `/onboarding` | Autenticada, propriedade/viveiros/dispositivo opcional |
| `/app` | Autenticada, visão geral com API real |

O onboarding persiste os IDs retornados em rascunho por usuário no `sessionStorage`. Voltar e avançar atualiza as entidades existentes; cada viveiro concluído é salvo antes do próximo. Não é uma transação única: uma queda de rede depois da gravação mas antes da resposta pode exigir conferir os cadastros existentes. A conclusão remove o rascunho.

O dashboard tem caches separados por usuário/propriedade/viveiro/período, atualização a cada minuto e gráficos sem dados demonstrativos. `active` significa cadastro ativo, não conexão online. A última leitura é indicada como antiga após 15 minutos. Ausência de alerta não equivale a certificação da qualidade da água. Os detalhes de dispositivos e alertas ficam na mesma tela; não foram criadas páginas extras de relatórios ou administração.

## API adicionada

`city` é opcional nos contratos de propriedade. É armazenada em `Tenant.settings.city`; PATCH omitindo cidade preserva, `null` remove apenas a cidade. Outros settings, inclusive valores nulos, permanecem.

`GET /v1/tenants/{tenant_id}/ponds/{pond_id}/metrics/summary?period=24h|7d|30d` exige a mesma associação/permissão das leituras atuais. Retorna:

```json
{
  "tenant_id": "tnt_...", "pond_id": "pond_...", "period": "24h", "interval": "5m",
  "series": [{"measured_at": "2026-09-19T15:00:00Z", "do_mg_l": 6.4, "ph": 7.3, "temp_c": 28.1}],
  "statistics": {
    "do_mg_l": {"mean": 6.1, "min": 4.8, "max": 7.9, "count": 1800},
    "ph": {"mean": 7.3, "min": 7.0, "max": 7.6, "count": 1800},
    "temp_c": {"mean": 28.1, "min": 27.0, "max": 29.0, "count": 1800}
  }
}
```

Janelas de 5 minutos para 24h; 1 hora para 7d e 30d. Estatísticas calculadas no Influx sobre todas as amostras originais, incluindo todos os dispositivos do viveiro; não sobre médias de janelas nem leituras limitadas. Sem amostras: série vazia, estatísticas nulas, contagem zero.

`POST /v1/leads` é público. Campos: `name`, `email`, `phone?`, `property_name?`, `source`, `consent: true`. Campos extras, inclusive CPF/cartão, são rejeitados. Só responde 201 depois de persistir no DynamoDB, partição `LEADS#YYYY-MM`, chave de ordenação data UTC + UUID. Nenhuma listagem pública. Redis indisponível retorna 503; excesso retorna 429 e `Retry-After`. Limite em janelas fixas de minuto por IP, padrão 5, configurável com `LEAD_RATE_LIMIT_PER_MINUTE`. Não armazena IP no lead; chave temporária usa hash do IP.

O IP vem de `request.client`, não de um cabeçalho encaminhado interpretado pela aplicação. Ao publicar atrás de proxy, configure `--proxy-headers --forwarded-allow-ips=<IPs reais do proxy>` no Uvicorn e restrinja acesso direto à API. Não use confiança irrestrita em cabeçalhos fornecidos pelo público. Leads não expiram automaticamente; a operação deve definir sua política de retenção e acesso.

### Exportar leads

Com credenciais AWS administrativas (cadeia padrão boto3, permissão `dynamodb:Query` na tabela):

```sh
uv run python scripts/admin/export_leads.py --start 2026-09-01 --end 2026-09-30 --output leads-setembro.csv
```

Datas inclusivas em UTC. Aceita `--table`, `--region`, `--endpoint-url`. Consulta cada partição mensal, percorre todas as páginas e escreve CSV com BOM UTF-8, escapando possíveis fórmulas de planilha. O arquivo de saída precisa ser novo. Não versionar exportações de contatos. Para DynamoDB local, use `AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local` e `--endpoint-url http://127.0.0.1:8001`.

## Verificação

```sh
npm run typecheck
npm test
npm run build
npm run test:visual
npm run test:production # usa o build em dist
```

Playwright pode precisar de `npx playwright install chromium`. A suíte visual inicia o Vite dev local se necessário. Fixtures HTTP vivem apenas em `e2e/fixtures.ts` e são usadas exclusivamente pela suíte visual; nenhum dado de teste é incluído no dashboard de produção. Relógio fixo, fontes locais carregadas, animações desativadas, desktop 1448×1086 ou 1086×1448 conforme referência e mobile 390×844. Capturas de viewport e página inteira em `artifacts/visual/screenshots/`.

Para regenerar comparações com as referências já copiadas:

```sh
node scripts/compare-visual.mjs
```

Na primeira execução pode passar o diretório das sete imagens como argumento. Abra `artifacts/visual/index.html` para alternar lado a lado, sobreposição e diferenças. Fotos reconstruídas e alterações de escopo geram diferenças intencionais; o percentual global de pixels não é um limiar de aprovação.

Com API, Vite e serviços locais em execução:

```sh
npm run test:local
# Na raiz:
RUN_FRONTEND_LOCAL_TESTS=1 uv run --extra dev pytest tests/integration/test_frontend_local.py -q
uv run --extra dev pytest -q
```

A integração real não intercepta HTTP. Cria registros com identificadores próprios na pilha local; não executar contra produção. Os dois testes Python confirmam 1.800 amostras reais no Influx, autorização, persistência e exportação real de leads.

## Publicação na mesma origem (sem deploy)

Gere `frontend/dist` com os IDs públicos corretos. Sirva os arquivos estáticos e encaminhe `/v1/` à API. O servidor precisa de fallback SPA (`try_files $uri $uri/ /index.html`) para todas as rotas. Sirva `/assets/` com cache imutável; `index.html` sem cache longo. Os fundos e fontes são locais. HTTPS é necessário em produção.

Aplique `X-Robots-Tag: noindex, nofollow` também no servidor para `/planos` e `/checkout`, reforçando o meta robots controlado pela aplicação para crawlers sem JavaScript. Não inclua essas URLs no sitemap. Exemplo de localização Nginx (adapte o root/upstream existente):

```nginx
location /v1/ { proxy_pass http://127.0.0.1:8000; }
location ~ ^/(planos|checkout)/?$ {
    add_header X-Robots-Tag "noindex, nofollow" always;
    try_files $uri /index.html;
}
location / { try_files $uri $uri/ /index.html; }
```

O nome/cidade da propriedade e tokens de autenticação nunca são incorporados ao build. Não foi realizado deploy.

## Documentação consultada

Context7 retornou quota mensal excedida. Consultadas fontes oficiais e tipos instalados:
- https://docs.amplify.aws/react/build-a-backend/auth/connect-your-frontend/manage-user-sessions/
- https://docs.amplify.aws/react/build-a-backend/auth/connect-your-frontend/sign-in/
- https://reactrouter.com/start/declarative/installation
- https://vite.dev/config/server-options
- https://github.com/TanStack/query/blob/main/docs/framework/react/guides/query-keys.md
