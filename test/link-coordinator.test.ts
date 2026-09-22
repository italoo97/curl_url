import { env, listDurableObjectIds, runInDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import type { LinkCoordinator } from '../src/durable-objects/link-coordinator.js';

function counterFor(slug: string) {
  const id = env.CLICKS.idFromName(slug);
  return { id, stub: env.CLICKS.get(id) };
}

async function countInD1(slug: string): Promise<number> {
  const row = await env.DB.prepare(
    'SELECT SUM(count) AS total FROM clicks_daily WHERE slug = ?',
  )
    .bind(slug)
    .first<{ total: number | null }>();
  return row?.total ?? 0;
}

describe('LinkCoordinator', () => {
  it('accumulates in the object instead of writing to D1 on every click', async () => {
    const { stub } = counterFor('acumula');

    await stub.increment();
    await stub.increment();
    await stub.increment();

    expect(await stub.pending()).toBe(3);
    // O ponto do ADR-005: três cliques, zero escritas no D1.
    expect(await countInD1('acumula')).toBe(0);
  });

  it('flushes the batch to D1 when the alarm fires', async () => {
    const { id, stub } = counterFor('alarme');

    await stub.increment();
    await stub.increment();

    await runInDurableObject(stub, async (instance: LinkCoordinator) => {
      await instance.alarm();
    });

    expect(await countInD1('alarme')).toBe(2);
    expect(await stub.pending()).toBe(0);
    void id;
  });

  it('adds to the same row instead of duplicating it on a second flush', async () => {
    const { stub } = counterFor('somaduas');

    await stub.increment();
    await runInDurableObject(stub, (i: LinkCoordinator) => i.alarm());
    await stub.increment();
    await runInDurableObject(stub, (i: LinkCoordinator) => i.alarm());

    // ON CONFLICT DO UPDATE somando, não uma linha nova por flush.
    expect(await countInD1('somaduas')).toBe(2);
    const rows = await env.DB.prepare(
      'SELECT COUNT(*) AS n FROM clicks_daily WHERE slug = ?',
    )
      .bind('somaduas')
      .first<{ n: number }>();
    expect(rows?.n).toBe(1);
  });

  it('keeps one counter per slug', async () => {
    await counterFor('slug-a').stub.increment();
    await counterFor('slug-b').stub.increment();
    await counterFor('slug-b').stub.increment();

    expect(await counterFor('slug-a').stub.pending()).toBe(1);
    expect(await counterFor('slug-b').stub.pending()).toBe(2);
    expect((await listDurableObjectIds(env.CLICKS)).length).toBeGreaterThanOrEqual(2);
  });

  it('does not write anything when there is nothing pending', async () => {
    const { stub } = counterFor('vazio');
    await runInDurableObject(stub, (i: LinkCoordinator) => i.alarm());
    expect(await countInD1('vazio')).toBe(0);
  });
});
