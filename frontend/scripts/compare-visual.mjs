import sharp from "sharp";
import pixelmatch from "pixelmatch";
import { mkdir, writeFile, copyFile } from "node:fs/promises";
import path from "node:path";
const root = path.resolve("../artifacts/visual");
const referenceDir = process.argv[2];
const names = {
  dashboard: "WhatsApp Image 2026-09-17 at 4.04.41 PM.jpeg",
  onboarding: "WhatsApp Image 2026-09-17 at 4.04.41 PM (1).jpeg",
  login: "WhatsApp Image 2026-09-17 at 4.04.42 PM.jpeg",
  checkout: "WhatsApp Image 2026-09-17 at 4.04.42 PM (1).jpeg",
  planos: "WhatsApp Image 2026-09-17 at 4.04.42 PM (2).jpeg",
  produto: "WhatsApp Image 2026-09-17 at 4.04.42 PM (3).jpeg",
  inicio: "WhatsApp Image 2026-09-17 at 4.04.42 PM (4).jpeg",
};
const notes = {
  inicio:
    "CTAs e faixa comercial substituídos por contato. Cartão ilustrativo identificado como demonstração. Fotografia, marca vetorial e ícones reconstruídos.",
  produto:
    "CTAs comerciais substituídos por contato; histórico apresentado sem prometer página extra de relatórios. Mantidas as quatro etapas e cartões de benefícios.",
  planos:
    "Rota oculta, noindex, preços apenas demonstrativos. CTA registra interesse. Preço anterior de Operação corrigido para coerência aritmética com a economia ilustrada.",
  checkout:
    "Sem cobrança ou criação de conta. CPF e cartão desativados. Campos de consentimento e contato alteram a altura e a disposição do formulário.",
  login:
    "Link comercial substituído por contato. Fotografia reconstruída; cartões explicitamente ilustrativos. Sem cadastro automático. Banner local oculto somente na captura visual.",
  onboarding:
    "Cidade livre e opcional em vez de lista predefinida. Foto reconstruída; estados adicionais do fluxo são funcionais. Dados preenchidos só no teste.",
  dashboard:
    "Dados controlados apenas nesta captura. Quatro viveiros correspondem às quatro linhas da fixture. Conexão desconhecida, alertas derivados de eventos e seletor de viveiro/período. Navegação limitada ao escopo; detalhes de dispositivos/alertas abaixo do painel.",
};
await mkdir(path.join(root, "comparisons"), { recursive: true });
await mkdir(path.join(root, "references"), { recursive: true });
const results = [];
for (const [name, filename] of Object.entries(names)) {
  const refFile = path.join(root, "references", `${name}.jpeg`);
  if (referenceDir) await copyFile(path.join(referenceDir, filename), refFile);
  const shot = path.join(root, "screenshots", `${name}-desktop.png`);
  const { width, height } = await sharp(refFile).metadata();
  const reference = await sharp(refFile).ensureAlpha().raw().toBuffer();
  const actual = await sharp(shot)
    .resize(width, height, { fit: "fill" })
    .ensureAlpha()
    .raw()
    .toBuffer();
  const diff = Buffer.alloc(reference.length);
  const count = pixelmatch(reference, actual, diff, width, height, {
    threshold: 0.15,
    includeAA: false,
  });
  await sharp(diff, { raw: { width, height, channels: 4 } })
    .png()
    .toFile(path.join(root, "comparisons", `${name}-diff.png`));
  const blend = Buffer.from(reference);
  for (let i = 0; i < blend.length; i += 4)
    for (let c = 0; c < 3; c++)
      blend[i + c] = Math.round((reference[i + c] + actual[i + c]) / 2);
  await sharp(blend, { raw: { width, height, channels: 4 } })
    .png()
    .toFile(path.join(root, "comparisons", `${name}-overlay.png`));
  await sharp({
    create: { width: width * 2, height, channels: 4, background: "#fff" },
  })
    .composite([
      { input: await sharp(refFile).png().toBuffer(), left: 0, top: 0 },
      {
        input: await sharp(actual, { raw: { width, height, channels: 4 } })
          .png()
          .toBuffer(),
        left: width,
        top: 0,
      },
    ])
    .png()
    .toFile(path.join(root, "comparisons", `${name}-side-by-side.png`));
  results.push({
    screen: name,
    width,
    height,
    differentPixels: count,
    differencePercent: Number(((count / (width * height)) * 100).toFixed(2)),
    notes: notes[name],
  });
}
await writeFile(
  path.join(root, "metrics.json"),
  JSON.stringify(results, null, 2) + "\n",
);
await writeFile(
  path.join(root, "index.html"),
  `<!doctype html><html lang="pt-BR"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>LimnoPulse — Comparação visual</title><style>body{font:15px system-ui;background:#edf5fa;color:#09283f;margin:30px auto;max-width:1300px;padding:0 20px}h1{font-size:30px}p{line-height:1.6}article{background:white;border:1px solid #d4e3ec;border-radius:12px;padding:24px;margin:25px 0}img{width:100%;height:auto;border:1px solid #e0e8ed}nav{display:flex;gap:15px;flex-wrap:wrap;margin:15px 0}a{color:#006dd1}.tabs{display:flex;gap:10px;margin:15px 0}button{padding:8px 15px;border:1px solid #bdcfdf;border-radius:6px;background:#f3f9ff;color:#075588;cursor:pointer}small{color:#62798b}</style><h1>LimnoPulse — Relatório visual</h1><p>À esquerda: referência fornecida. À direita: implementação React. Capturas com relógio fixo, fontes locais carregadas e animações desativadas. O percentual de diferença inclui fotografias reconstruídas, textos e mudanças intencionais; não é um critério de aprovação automática.</p><nav>${results.map((r) => `<a href="#${r.screen}">${r.screen}</a>`).join("")}</nav>${results.map((r) => `<article id="${r.screen}"><h2>${r.screen}</h2><p>${r.notes}</p><small>${r.width} × ${r.height} · ${r.differencePercent}% dos pixels diferem com limiar 0,15 (antialiasing excluído)</small><div class="tabs"><button data-screen="${r.screen}" data-kind="side-by-side">Lado a lado</button><button data-screen="${r.screen}" data-kind="overlay">Sobreposição 50%</button><button data-screen="${r.screen}" data-kind="diff">Diferenças</button></div><img id="img-${r.screen}" src="comparisons/${r.screen}-side-by-side.png" alt="Comparação ${r.screen}" loading="lazy"><nav><a href="screenshots/${r.screen}-desktop-full.png">Desktop completo</a><a href="screenshots/${r.screen}-mobile-full.png">Mobile 390 × 844</a><a href="references/${r.screen}.jpeg">Referência</a></nav></article>`).join("")}<script>document.querySelectorAll('button[data-kind]').forEach(b=>b.onclick=()=>document.getElementById('img-'+b.dataset.screen).src='comparisons/'+b.dataset.screen+'-'+b.dataset.kind+'.png')</script></html>`,
);
console.log(
  results.map((r) => `${r.screen}: ${r.differencePercent}%`).join("\n"),
);
