# curl_url

[![CI](https://github.com/italoo97/curl_url/actions/workflows/ci.yml/badge.svg)](https://github.com/italoo97/curl_url/actions/workflows/ci.yml)
[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-F38020.svg)](https://developers.cloudflare.com/workers/)
[![TypeScript](https://img.shields.io/badge/TypeScript-strict-3178C6.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A URL shortener running on Cloudflare Workers — built as an exercise in
picking the right storage primitive for each job, rather than putting
everything in one place.

The interesting part is not the shortening. It's that a service this small
needs **four different storage products for four different reasons**, and that
each choice has to survive the question *"why not the simpler thing?"*.

https://curl-url.italohugodf39.workers.dev

## Why these services

| Concern | Product | Why not something simpler |
|---|---|---|
| `slug → URL` | **KV** | The canonical case: written once, read from everywhere, and nobody cares if a brand-new link takes a few seconds to propagate globally |
| Click counting and single-use claims | **Durable Object** per slug | KV caps writes to **one per second on the same key** — a viral link would drop counts silently. A D1 write per click would serialise, since each D1 database is single-threaded. The DO absorbs the contention and flushes batches |
| Stats queries | **D1** | Needs `WHERE`, `GROUP BY` and date ranges. KV only looks up by key |
| QR code per slug | **R2** | User-generated, unbounded in number, and immutable — the slug never changes, so the file never does |
| Bot protection on creation | **Turnstile** | Creation is open on purpose; an account wall would defeat a demo, and nothing else keeps a script from filling KV |
| Interstitial / front-end | **Static assets** on the Worker | No shared state, no coordination. A Durable Object here would be exactly the kind of unjustified usage this project exists to avoid |
| Expiring old links | **Cron Trigger** | One line of configuration — and a capability Pages does not have |

The row that matters most is the one where a service was **not** used.

Considered and rejected: **Analytics Engine**, which is the product actually
designed for event time-series like clicks and is probably the better choice
in a production system. Passed over here because DO + D1 is explainable in one
sentence and gives explicit control over the data model.

Full reasoning, including the alternatives rejected for each decision, is in
[`docs/DECISOES.md`](docs/DECISOES.md) (Portuguese).

## Architecture

```
                      ┌─────────────────────────────┐
client ──GET /:slug──▶│  Worker                     │
                      │  1. KV.get(link:<slug>)     │──▶ KV
                      │  2. DO(slug).increment()    │──▶ Durable Object
                      │  3. 302 Location            │         │ batched flush
                      └─────────────────────────────┘         ▼
                                                              D1
```

## Status

All seven phases done. Creation is open to anyone, protected by Cloudflare
Turnstile rather than by an account wall — the point of the project is that
people can try it.

| Phase | Scope | State |
|---|---|---|
| 1 | Create link + redirect, on KV | ✅ done |
| 2 | Click counting via Durable Object, batched into D1 | ✅ done |
| 3 | Stats endpoint | ✅ done |
| 4 | QR codes in R2 | ✅ done |
| 5 | Expiry + orphan sweep | ✅ done |
| 6 | Single-use links (atomic check-and-set) | ✅ done |
| 7 | Front-end + Turnstile | ✅ done |

## Getting started

Requires Node 22+ (see `.nvmrc`) and a Cloudflare account. The package manager
version is pinned in `package.json`, so Corepack installs the right pnpm
automatically.

```bash
corepack enable
pnpm install

# Create the KV namespace and paste the printed id into wrangler.jsonc
npx wrangler kv namespace create LINKS

cp .dev.vars.example .dev.vars   # Turnstile test secret, always passes

pnpm dev       # local dev server, no Cloudflare account touched
pnpm gate      # lint + type check + tests
pnpm deploy    # publish

# In production, the real secret never goes in a file:
npx wrangler secret put TURNSTILE_SECRET_KEY
```

## API

### `POST /api/links`

```bash
curl -X POST http://localhost:8787/api/links \
  -H 'content-type: application/json' \
  -d '{"url": "https://example.com/a/very/long/link"}'
```

```json
{
  "slug": "k3mQp7x",
  "url": "https://example.com/a/very/long/link",
  "shortUrl": "http://localhost:8787/k3mQp7x"
}
```

Rejects, with `422`, anything that is not `http`/`https`, is longer than 2048
characters, or resolves to a private address. That last one matters: without
it a public shortener becomes an SSRF façade — someone shortens
`http://169.254.169.254/latest/meta-data/` and uses your domain to reach it.

### `GET /api/links/:slug/stats`

```json
{
  "slug": "k3mQp7x",
  "url": "https://example.com/a/very/long/link",
  "createdAt": "2026-09-22T01:00:00.000Z",
  "total": 5,
  "pending": 3,
  "byDay": [{ "day": "2026-09-22", "count": 2 }]
}
```

`total` is the honest number at the moment of the request: what D1 already
holds, plus what the Durable Object is still accumulating. `byDay` only
reflects flushed batches, so it can trail `total` by up to the flush window.

That gap is reported rather than hidden. `pending` is exactly the difference,
so a consumer can tell a stale aggregate from a wrong one — the alternative
would be a `total` that silently lags by ten seconds and looks like a bug.

`turnstileToken` is required. `expiresIn` (seconds, 60 to one year) and
`oneTime` (boolean) are optional; the response echoes the resulting
`expiresAt` and `oneTime`.

### `GET /api/links/:slug/qr`

Returns an SVG QR code for the short link, generated on first request and
cached in R2 from then on (`x-qr-cache: miss` then `hit`).

Two choices worth naming. **SVG, not PNG**: a QR code is a grid of squares —
vector by nature. Rasterising would mean shipping a PNG encoder into the
Worker to produce a larger file that blurs when enlarged. **On demand, not at
creation**: most shortened links are never shared as a QR, so generating one
per link would fill the bucket with objects nobody asks for. The first
request pays for generation; every one after reads the object.

The QR encodes the **short** URL, not the destination. Encoding the
destination would bypass the Worker — the scan would not be counted, and
changing where the link points would mean reprinting everything already in
circulation.

### `GET /:slug`

`302` to the stored URL, `404` if unknown, `410` if expired.

The redirect is deliberately a `302`, not a `301`. A `301` is cached by the
browser indefinitely, so after the first click the request would never reach
the Worker again — and click counting would quietly stop working for everyone
who had already used the link.

### Counting without slowing anybody down

The click is recorded through `ctx.waitUntil()`, so the `302` leaves before
the counter is touched — measuring must not cost latency to the person who
clicked. The Durable Object accumulates in durable storage and flushes to D1
on an alarm (10s) or at 100 pending clicks, whichever comes first, with an
`ON CONFLICT DO UPDATE` so a second flush adds to the day's row instead of
duplicating it.

The test that justifies the whole design fires twenty concurrent requests at
the same slug and asserts all twenty are counted. The same test against KV
would fail: one write per second, same key.

### Open, but not defenceless

Anyone can shorten a link — no account, no key. That is the point: a demo
behind a login is a demo nobody tries. What stands between the endpoint and a
script is [Turnstile](https://developers.cloudflare.com/turnstile/).

Three things make that verification real rather than decorative:

- **The widget alone protects nothing.** Anyone can POST straight to
  `/api/links` with an arbitrary string. The server calls Siteverify, and
  that call is what decides.
- **It fails closed.** With no `TURNSTILE_SECRET_KEY` configured, creation
  answers `503`. A deploy that forgot the secret is an outage, not an open
  door — and an outage gets noticed.
- **A network failure is not a pass.** If Siteverify times out, the request
  is refused. Being unable to check is not evidence of being human.

Verification runs on creation only. A redirect must never show a challenge.

Local development uses Cloudflare's published test keys, which work on any
hostname and always pass — see `.dev.vars.example`.

### Single-use links, and why they need a Durable Object

A link created with `oneTime: true` redirects once and answers `410 Gone`
after that. This is the one feature here that KV simply cannot implement
correctly: two simultaneous requests would both read "unused" before either
write landed, and both would be let through. The Durable Object serialises
the read-and-write pair, so "first" actually means something.

The test fires ten concurrent requests at the same link and asserts exactly
one `302` and nine `410`s.

Note the contrast with click counting, which uses the *same* Durable Object.
Counting goes through `ctx.waitUntil()` and never delays the response, because
losing a count costs one row of statistics. Claiming is awaited on the
critical path, because letting two people use a single-use link is precisely
the bug the feature exists to prevent. Same primitive, opposite latency
treatment, decided by what being wrong costs.

### Expiry: what the platform does, and what it leaves behind

KV expires keys natively, so `expiresIn` is passed straight to
`expirationTtl` and the link deletes itself. There is no cron sweeping
expired links, because writing one would be reimplementing something the
platform already does.

What KV does *not* do is clean up the other services. When a key expires, its
`clicks_daily` rows in D1 and its `qr/<slug>.svg` object in R2 stay there
forever, pointing at a slug that no longer exists. That orphan is what the
Cron Trigger collects, nightly.

`expiresAt` is still stored inside the value, for two reasons: the API can
report it, and the redirect can answer `410 Gone` rather than `404` during
the window between the stated expiry and KV's actual deletion.

**Known limitation.** The sweep scans distinct slugs in D1 and checks each
against KV, capped at 500 per run. That is fine at this scale and wrong at a
large one — past a certain volume the right design records deletions instead
of scanning for them.

## Development

```bash
pnpm lint        # Biome (lint + format check)
pnpm format      # Biome, writing fixes
pnpm typecheck   # tsc --noEmit, strict
pnpm test        # Vitest, inside the real workerd runtime
pnpm gate        # all of the above, in order
```

Tests run in **workerd itself**, through `@cloudflare/vitest-pool-workers`,
with the bindings declared in `wrangler.jsonc` and isolated per-test storage.
The KV a test writes to has the same API as the one in production — no mock
to drift out of sync, and no network.

`compatibility_date` is pinned to a date the bundled test runtime supports.
The workerd binary inside Miniflare trails the latest date by a few weeks;
raising one without the other fails with `ERR_RUNTIME_FAILURE`.

### Conventions

Commits follow [Conventional Commits](https://www.conventionalcommits.org/),
enforced by commitlint on a `commit-msg` hook. Versioning and the changelog
are produced from those commits by release-please — the version in
`package.json` is never edited by hand.

CI runs lint, type check and tests as separate steps, so a red build says
which one failed without opening the log. Deploys run the same gate again
before publishing.

## License

MIT
