import {
  createExecutionContext,
  env,
  runInDurableObject,
  waitOnExecutionContext,
} from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import type { LinkCoordinator } from '../src/durable-objects/link-coordinator.js';
import worker from '../src/index.js';

const ctx = createExecutionContext();

interface StatsBody {
  slug: string;
  url: string;
  total: number;
  pending: number;
  byDay: { day: string; count: number }[];
}

async function createLink(url: string): Promise<string> {
  const response = await worker.fetch(
    new Request('https://curl.test/api/links', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ url, turnstileToken: 'ok' }),
    }),
    env,
    ctx,
  );
  return ((await response.json()) as { slug: string }).slug;
}

async function click(slug: string, times = 1): Promise<void> {
  for (let i = 0; i < times; i++) {
    await worker.fetch(new Request(`https://curl.test/${slug}`), env, ctx);
  }
  await waitOnExecutionContext(ctx);
}

async function flush(slug: string): Promise<void> {
  const stub = env.CLICKS.get(env.CLICKS.idFromName(slug));
  await runInDurableObject(stub, (instance: LinkCoordinator) => instance.alarm());
}

async function getStats(slug: string) {
  const response = await worker.fetch(
    new Request(`https://curl.test/api/links/${slug}/stats`),
    env,
    ctx,
  );
  return { response, body: (await response.json()) as StatsBody };
}

describe('GET /api/links/:slug/stats', () => {
  it('reports zero for a link nobody clicked', async () => {
    const slug = await createLink('https://exemplo.com/parado');
    const { response, body } = await getStats(slug);

    expect(response.status).toBe(200);
    expect(body.total).toBe(0);
    expect(body.byDay).toEqual([]);
    expect(body.url).toBe('https://exemplo.com/parado');
  });

  // O ponto do endpoint: o total é verdadeiro no instante da consulta, mesmo
  // com o lote ainda dentro do Durable Object.
  it('counts clicks still pending in the Durable Object', async () => {
    const slug = await createLink('https://exemplo.com/pendente');
    await click(slug, 3);

    const { body } = await getStats(slug);

    expect(body.total).toBe(3);
    expect(body.pending).toBe(3);
    expect(body.byDay).toEqual([]);
  });

  it('moves the clicks into byDay once they are flushed', async () => {
    const slug = await createLink('https://exemplo.com/descarregado');
    await click(slug, 2);

    // alarm() é handler do runtime, não método RPC: só dá para dispará-lo
    // de dentro da instância.
    await flush(slug);

    const { body } = await getStats(slug);

    expect(body.total).toBe(2);
    expect(body.pending).toBe(0);
    expect(body.byDay).toHaveLength(1);
    expect(body.byDay[0]?.count).toBe(2);
  });

  it('adds flushed and pending together', async () => {
    const slug = await createLink('https://exemplo.com/misto');
    await click(slug, 2);
    await flush(slug);
    await click(slug, 3);

    const { body } = await getStats(slug);

    expect(body.total).toBe(5);
    expect(body.pending).toBe(3);
    expect(body.byDay[0]?.count).toBe(2);
  });

  it('returns 404 for an unknown slug', async () => {
    const { response } = await getStats('naoexiste');
    expect(response.status).toBe(404);
  });

  it('refuses a method other than GET', async () => {
    const slug = await createLink('https://exemplo.com/metodo');
    const response = await worker.fetch(
      new Request(`https://curl.test/api/links/${slug}/stats`, { method: 'DELETE' }),
      env,
      ctx,
    );
    expect(response.status).toBe(405);
  });
});
