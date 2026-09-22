# curl_url — Encurtador de URL na Cloudflare

> Documento de trabalho: decisões, arquitetura, roadmap e registro de progresso.
> Mantido em português. Quando o repositório for público, o `README.md` sai em
> inglês, como nos outros projetos.

**Início:** 22/09/2026
**Repositório atual:** `github.com/italoo97/curl_url` (versão Flask + DuckDB, a ser substituída)

---

## 1. Objetivo

Dois, nesta ordem de prioridade:

1. **Ter um link vivo.** Um encurtador sempre no ar, de graça, que um recrutador
   abre e usa em dez segundos. Hoje o repositório é Flask + DuckDB, que não se
   hospeda gratuitamente em lugar nenhum — ou seja, é um projeto morto no
   portfólio.
2. **Demonstrar uso justificado do ecossistema Cloudflare.** Cada serviço
   presente precisa de um motivo que sobreviva à pergunta *"por que não o mais
   simples?"*. Serviço usado sem motivo é sinal negativo, não neutro: quem
   revisa passa a duvidar de todas as outras escolhas.

**Não é objetivo:** ser um produto. Não compete com bit.ly, não tem painel de
usuário, não tem billing.

---

## 2. Decisões

Formato enxuto de ADR. A seção *Alternativas rejeitadas* é a parte importante —
é o que mostra que houve escolha.

### ADR-001 — Reescrita, não porte

**Contexto.** A versão atual é Flask + DuckDB.
**Decisão.** Reescrever do zero como Worker.
**Por quê.** Flask depende de WSGI e sockets; DuckDB é binário nativo. Nenhum
dos dois roda em Worker. Não existe caminho de migração — só de substituição.
**Consequência.** O código antigo fica no histórico do git. O README novo
explica a troca.

### ADR-002 — Workers, não Pages

**Contexto.** Pages é a opção óbvia para quem pensa "site estático + API".
**Decisão.** Workers com static assets (`assets.directory` no `wrangler.jsonc`).
**Por quê.** A própria Cloudflare mantém um guia de migração de Pages para
Workers. Na matriz de compatibilidade deles, Workers tem tudo que Pages tem e
mais: Cron Triggers, Durable Objects, Queue Consumers, Rate Limiting, Workers
Logs, Logpush, deploys graduais. Assets estáticos são gratuitos nos dois.
**Alternativas rejeitadas.** Pages — perderia Cron Trigger e Durable Object,
que são duas peças centrais deste desenho.
**Exceção conhecida.** Pages suporta domínio cujos nameservers estão fora da
Cloudflare. Não é o nosso caso.

### ADR-003 — TypeScript, não Python

**Contexto.** Python Workers existe e é first-class: dá para escrever KV, D1,
R2, Durable Objects, Cron e Workflows inteiramente em Python.
**Decisão.** TypeScript.
**Por quê.** A decisão não é de capacidade, é de carreira. A única parte difícil
deste projeto — saber quando usar KV, DO e D1 — não muda com a linguagem. Sendo
a lógica trivial, a linguagem passa a ser a variável de maior retorno:

- O portfólio tem 12 repositórios em Python e essencialmente nada em TS. "Só
  Python" é objeção real de recrutador, e um encurtador é o projeto mais barato
  que vai aparecer para derrubá-la.
- Vaga que pede Cloudflare quase sempre pede JS/TS junto.
- O ecossistema de exemplos de Workers é TS; travar custa menos.

**Nota técnica.** Python Workers roda sobre Pyodide e exige um helper de
conversão (`pyodide.ffi.to_js`) nas fronteiras com APIs JavaScript. Escolher
Python não elimina o JavaScript — só o move para uma fronteira mais
desconfortável.
**Consequência.** Python Workers fica reservado para um projeto onde seja a
escolha certa e não a confortável: levar parte do `study-mcp` para a borda com
Vectorize e Workers AI.

### ADR-004 — KV para o mapeamento `slug → URL`

**Decisão.** KV.
**Por quê.** É o caso de uso canônico: escreve uma vez, lê muitas, e o dado
fica replicado perto do usuário em qualquer região. Consistência eventual é
irrelevante aqui — ninguém liga se um link recém-criado leva alguns segundos
para propagar globalmente.
**Limites que aceitamos.** 100.000 leituras/dia e 1.000 escritas/dia no plano
free. Para um demo de portfólio, sobra muito.

### ADR-005 — Durable Object para contar cliques (não KV, não D1 direto)

Esta é a decisão que dá substância ao projeto.

**Contexto.** Contador é escrita concorrente na mesma entidade.
**Decisão.** Um Durable Object por slug, acumulando em memória e fazendo flush
em lote para o D1.
**Por quê.**

- **KV não conta.** O limite é de **1 escrita por segundo na mesma chave**, nos
  dois planos. Dez cliques no mesmo segundo perdem escritas, silenciosamente.
- **D1 direto serializa.** Cada banco D1 é single-threaded e processa uma query
  por vez; a vazão sai da duração da query. Um link viral enfileira e depois
  devolve erro de "overloaded".
- **DO resolve na origem.** Instância única e endereçável por slug, com
  consistência forte, serializando as escritas daquele slug. Absorve o hot
  write e o D1 recebe lote.

**Alternativas rejeitadas.** Analytics Engine — é o produto desenhado para série
temporal de eventos e provavelmente a escolha certa em produção real. Preterido
aqui porque DO + D1 é explicável em uma frase numa entrevista e dá controle
explícito sobre o modelo de dados. **Citar essa alternativa no README.**

### ADR-006 — D1 para consulta de estatísticas

**Decisão.** D1 recebe os lotes do DO e atende as consultas.
**Por quê.** Estatística exige `WHERE`, `GROUP BY` e recorte por período. KV não
consulta — só busca por chave.
**Limites.** 500 MB por banco e 50 queries por invocação no free; 10 GB por
banco no pago, e esse teto **não pode ser aumentado**. Agregação por dia, nunca
uma linha por clique.

### ADR-007 — R2 só para o QR code

**Decisão.** R2 guarda o QR code de cada slug. Nada mais.
**Por quê.** É arquivo gerado por usuário, em quantidade ilimitada — não se
versiona no git. E o QR de um slug é **estável**: o slug não muda, então o
arquivo é imutável e cacheável para sempre. Vale guardar em vez de regerar.
**Regra geral adotada.** *R2 é para o que é caro de regenerar ou precisa ser
imutável. Não guarde dado derivado.*
**Contraste que prova a regra.** Num sistema de pagamento, o QR de uma cobrança
Pix **não** vai para o R2: o dado real é o payload BR Code (texto), a imagem é
derivada e barata de regerar, e a cobrança expira. Mesma tecnologia, decisão
oposta — o que decide é o ciclo de vida do dado.
**Nota operacional.** O endpoint `r2.dev` do bucket é só para teste (rate limit
e throttle de banda). Em produção, domínio custom.

### ADR-008 — A página intersticial é Worker puro

**Contexto.** A ideia original era usar um Durable Object para mostrar um
anúncio antes de liberar o redirecionamento.
**Decisão.** Worker comum devolvendo HTML, servido dos static assets.
**Por quê.** Não há estado compartilhado, coordenação nem necessidade de
consistência forte. Um DO ali seria exatamente o tipo de uso sem motivo que este
projeto existe para *não* demonstrar.
**Registrar no README.** Esta linha — onde decidimos **não** usar um serviço —
vale mais que as outras.

### ADR-009 — Cron Trigger para recolher órfãos

> **Revisto na implementação (22/09).** A decisão original dizia que o cron
> apagaria "links vencidos do KV". Estava errada: **o KV tem TTL nativo** e
> apaga a chave sozinho. Escrever um cron para isso seria reimplementar o que
> a plataforma já faz.

**Decisão.** O vencimento do link fica a cargo do `expirationTtl` do KV. O
Cron Trigger diário cuida do que o KV **não** cobre: as linhas de
`clicks_daily` no D1 e o objeto `qr/<slug>.svg` no R2, que sobrevivem à
chave e ficam apontando para um slug inexistente.

**Por quê.** É a divisão honesta de responsabilidade: usar o recurso da
plataforma onde ele existe, e escrever código só para a lacuna que ele
deixa. O cron continua justificando sua presença (e continua sendo algo que
o Pages não tem, reforçando o ADR-002), mas agora por um motivo real.

**Limitação conhecida.** A varredura lê os slugs distintos do D1 e confere
cada um contra o KV, com teto de 500 por execução. Funciona nesta escala e
seria errado em escala grande -- ali o certo é registrar a remoção em vez de
varrer procurando.

### ADR-011 — Criação aberta, protegida por Turnstile

**Contexto.** O objetivo nº 1 do projeto é que pessoas usem o link. Exigir
conta mataria isso. Deixar aberto sem nada vira brinquedo de script em uma
semana -- e o KV tem 1.000 escritas/dia no plano free.
**Decisão.** Criação aberta a qualquer um, com verificação Turnstile
obrigatória no `POST /api/links`.
**Alternativas rejeitadas.** *Conta/API key*: mata o propósito. *Só rate
limit por IP*: um script distribuído passa por baixo, e ainda gastaria a
cota. *Só o widget no front*: não protege nada -- qualquer um faz POST
direto no endpoint.

**Três detalhes que fazem a diferença entre proteção e enfeite:**

1. *A validação é no servidor.* O widget produz um token; quem decide é a
   chamada ao Siteverify, feita no backend. Expor a secret no front
   devolveria o problema para quem se quer barrar.
2. *Falha fechada.* Sem `TURNSTILE_SECRET_KEY`, o endpoint responde 503 em
   vez de liberar. Um deploy que esqueceu a secret vira indisponibilidade --
   que alguém percebe -- e não porta aberta, que ninguém percebe.
3. *Erro de rede não é aprovação.* Timeout no Siteverify recusa a
   requisição. Não conseguir verificar não é evidência de ser humano.

**Só na criação.** Redirect nunca mostra desafio.

**Testabilidade.** As chaves de teste publicadas pela Cloudflare funcionam em
qualquer hostname; nos testes automatizados a saída do Worker é interceptada
pelo `outboundService` do miniflare, então nenhuma requisição sai da máquina.

### ADR-010 — Link de uso único via Durable Object

**Decisão.** Feature opcional: link que só pode ser aberto uma vez.
**Por quê.** Exige check-and-set atômico. É **impossível** de fazer correto no
KV — dois cliques simultâneos passariam os dois. O DO é a única peça do
ecossistema que resolve, e é uma feature que qualquer pessoa entende olhando.

---

## 3. Arquitetura

```
                      ┌─────────────────────────────┐
cliente ──GET /:slug──│  Worker                     │
                      │  1. KV.get(link:<slug>)     │──→ KV
                      │  2. DO(slug).increment()    │──→ Durable Object
                      │  3. 302 Location            │      │ flush em lote
                      └─────────────────────────────┘      ↓
                                                          D1
```

| Camada | Serviço | A razão que sobrevive à pergunta |
|---|---|---|
| `slug → URL` | **KV** | Lê muito, escreve uma vez, latência global |
| Contagem de cliques | **Durable Object** por slug | Absorve escrita concorrente que KV não suporta e D1 serializaria |
| Consulta de estatísticas | **D1** | Precisa de `WHERE`, `GROUP BY`, recorte por período |
| QR code por slug | **R2** | Gerado por usuário, imutável, quantidade ilimitada |
| Página de anúncio / front | **Worker + static assets** | Não tem estado; DO aqui seria exagero |
| Link de uso único | **Durable Object** | Check-and-set atômico, impossível no KV |
| Expurgo de vencidos | **Cron Trigger** | Uma linha de config; Pages não tem |

### Modelo de dados

**KV** — chave `link:<slug>`

```json
{
  "url": "https://exemplo.com/caminho/longo",
  "createdAt": "2026-09-22T01:00:00Z",
  "expiresAt": null,
  "oneTime": false
}
```

**Durable Object** — um por slug, classe `ClickCounter`

```
storage: { count: number, pendingSince: number }
```

Acumula em memória, faz flush para o D1 por alarme (a cada N segundos ou N
cliques, o que vier primeiro).

**D1**

```sql
CREATE TABLE links (
  slug       TEXT PRIMARY KEY,
  url        TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT
);

-- Agregado por dia. Nunca uma linha por clique: o teto de 10 GB por banco
-- é rígido e não pode ser aumentado.
CREATE TABLE clicks_daily (
  slug  TEXT NOT NULL,
  day   TEXT NOT NULL,
  count INTEGER NOT NULL,
  PRIMARY KEY (slug, day)
);
```

**R2** — chave `qr/<slug>.png`

---

## 4. Limites que moldaram o desenho

Anotados porque são a justificativa das decisões, não trivia.

| Serviço | Limite relevante |
|---|---|
| KV | **1 escrita/s na mesma chave** (free e pago) · 1.000 escritas/dia (free) · 100k leituras/dia (free) · valor até 25 MiB |
| D1 | 500 MB por banco (free) · 10 GB por banco (pago, **teto rígido**) · single-threaded por banco · 50 queries por invocação (free) |
| R2 | Objetos ilimitados · 1 escrita/s na mesma chave · `r2.dev` com rate limit, não usar em produção |
| Durable Objects | Consistência forte, serializa por instância |

---

## 5. Roadmap

### Fase 0 — Scaffold
`wrangler.jsonc`, TypeScript, um `fetch` devolvendo 200, `wrangler dev` local e
primeiro `wrangler deploy`.
**Pronto quando:** existe uma URL `*.workers.dev` no ar.

### Fase 1 — Núcleo
`POST /api/links` cria slug e grava no KV. `GET /:slug` lê e devolve 302.
Validação de URL de entrada. 404 para slug inexistente.
**Pronto quando:** dá para encurtar um link pela API e ser redirecionado.

### Fase 2 — Contagem ✅
Durable Object `ClickCounter` por slug, incremento no redirect, flush em lote
para o D1 por alarme.
**Pronto quando:** cliques simultâneos são contados sem perda. ✅ Verificado:
20 requisições em paralelo contra o mesmo slug, as 20 contadas.

**Aprendido na implementação**, três coisas que não estavam previstas:
o alarme precisa de guarda (`getAlarm() === null`), senão cada clique
reagenda e o flush é adiado indefinidamente; o nome do DO (`ctx.id.name`) já
carrega o slug, dispensando duplicá-lo no storage; e o input gating do
runtime dispensa lock em torno do par ler/zerar do flush.

### Fase 3 — Estatísticas ✅
`GET /api/links/:slug/stats` consultando o D1, agregado por dia.
**Pronto quando:** o total do endpoint casa com o número de requisições
feitas. ✅ Verificado, inclusive com lote ainda não descarregado.

**Decisão tomada aqui.** O endpoint lê de duas fontes e soma: o D1 tem o que
já foi descarregado, o DO tem o que ainda acumula. Só o D1 daria um total
atrasado em até dez segundos -- que o usuário leria como bug, não como
latência. A diferença vai explícita no campo `pending`, para quem consome a
API distinguir agregado defasado de número errado.

### Fase 4 — QR code ✅
Geração **sob demanda** (decisão revista: era "no ato da criação"), gravação
em R2, servido por rota própria.
**Pronto quando:** o QR abre o link ao ser escaneado. ✅ Conteúdo verificado
em teste contra o payload esperado.

**Duas decisões tomadas aqui.**

*SVG em vez de PNG.* O ADR-007 falava em `qr/<slug>.png`. Um QR é uma grade
de quadrados -- vetorial por natureza. Rasterizar exigiria embarcar um
codificador PNG no Worker para gerar um arquivo maior, que borra ao ampliar e
ainda obrigaria a escolher uma resolução. A chave virou `qr/<slug>.svg`.

*Sob demanda em vez de na criação.* A maioria dos links nunca é compartilhada
por QR; gerar na criação encheria o bucket de objetos que ninguém pede. O
primeiro acesso paga a geração, os seguintes leem do R2. Só foi possível
fazer isso agora porque a Fase 3 fechou o contrato do slug -- gerar antes
seria criar arquivo imutável de algo ainda em movimento.

*Biblioteca:* `uqr`, sem dependências, roda em qualquer runtime.

### Fase 5 — Expurgo ✅
Cron Trigger diário recolhendo órfãos no D1 e no R2 (o link em si expira pelo
TTL nativo do KV -- ver ADR-009 revisto).
**Pronto quando:** um slug ausente do KV tem suas linhas do D1 e seu objeto
do R2 removidos, e um slug vivo fica intocado. ✅ Ambos em teste, mais
idempotência de execuções repetidas.

### Fase 6 — Link de uso único ✅
Check-and-set atômico no DO.
**Pronto quando:** requisições simultâneas ao mesmo link de uso único
resultam em um sucesso e o resto 410. ✅ Verificado com 10 em paralelo:
exatamente um 302, nove 410.

**Renomeação com migration.** A classe passou a fazer duas coisas por slug
(contar e reivindicar), então `ClickCounter` virou `LinkCoordinator`. Renomear
classe de Durable Object exige entrada `renamed_classes` nas migrations --
sem ela o runtime entende que uma classe sumiu e outra nasceu, e o storage
das instâncias existentes seria descartado.

**Assimetria deliberada.** O mesmo DO atende as duas operações com
tratamentos opostos de latência: `increment()` vai por `ctx.waitUntil` e não
atrasa nada, porque perder um clique custa uma linha de estatística;
`claim()` é aguardado no caminho crítico, porque deixar dois usarem o mesmo
link é o bug que a feature existe para impedir. O que decide não é a
primitiva, é o custo de errar.

### Fase 7 — Vitrine ✅
Front em static assets com widget do Turnstile, opções de uso único e
expiração, e links para QR e estatísticas. `README.md` público em inglês com
a seção *"Why these services"*.
**Pronto quando:** um recrutador entende o projeto sem rodar nada. ✅

---

## 6. Pendências e decisões abertas

- **Nome do Worker e domínio.** `workers.dev` resolve; domínio próprio é opcional.
- ~~Criação de link é aberta ou autenticada?~~ **Resolvido: aberta, com
  Turnstile.** Ver ADR-011.
- **Reaproveitar o repositório `curl_url` ou criar novo?** Reaproveitar mantém a
  narrativa da reescrita visível no histórico. Inclinação: reaproveitar.
- **Slug customizado** pelo usuário: fase futura, exige tratar colisão.

---

## 7. Registro de progresso

| Data | Fase | O que aconteceu |
|---|---|---|
| 22/09/2026 | — | Decisões ADR-001 a ADR-010 fechadas. Documento criado. |
| 22/09/2026 | 0 e 1 | Scaffold (pnpm, Biome, tsc strict, Vitest em workerd, husky, commitlint, release-please, CI/CD). Núcleo no KV: `POST /api/links` e `GET /:slug`. Gate verde, 27 testes. |
| 22/09/2026 | 7 | Criação aberta protegida por Turnstile (falha fechada), `/api/config` entregando a sitekey, front com widget e opções de uso único/expiração. 68 testes. |
| 22/09/2026 | 6 | Link de uso único via `claim()` atômico. `ClickCounter` renomeado para `LinkCoordinator` com migration `renamed_classes`. 62 testes. |
| 22/09/2026 | 5 | `expiresIn` no POST usando o TTL nativo do KV, e Cron Trigger recolhendo órfãos no D1/R2. ADR-009 revisto. 58 testes. |
| 22/09/2026 | 4 | `GET /api/links/:slug/qr`: SVG gerado sob demanda com `uqr` (zero dependências) e cacheado no R2 sob `qr/<slug>.svg`. 47 testes. |
| 22/09/2026 | 3 | `GET /api/links/:slug/stats` somando o agregado do D1 com o pendente do DO, e expondo a diferença em `pending`. 41 testes. |
| 22/09/2026 | 2 | `ClickCounter` (Durable Object) por slug, flush em lote no D1 por alarme (10s) ou 100 cliques. Incremento via `ctx.waitUntil`, fora do caminho de resposta. 35 testes, incluindo 20 cliques simultâneos sem perda. |
