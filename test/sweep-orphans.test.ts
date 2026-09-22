import { env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { sweepOrphans } from '../src/jobs/sweep-orphans.js';
import type { StoredLink } from '../src/types.js';

async function seedClicksAndQr(slug: string): Promise<void> {
  await env.DB.prepare(
    'INSERT OR REPLACE INTO clicks_daily (slug, day, count) VALUES (?, ?, ?)',
  )
    .bind(slug, '2026-09-22', 7)
    .run();
  await env.QR.put(`qr/${slug}.svg`, '<svg/>');
}

async function seedLink(slug: string): Promise<void> {
  await env.LINKS.put(
    `link:${slug}`,
    JSON.stringify({
      url: 'https://exemplo.com/',
      createdAt: new Date().toISOString(),
      expiresAt: null,
      oneTime: false,
    } satisfies StoredLink),
  );
}

async function clickRows(slug: string): Promise<number> {
  const row = await env.DB.prepare(
    'SELECT COUNT(*) AS n FROM clicks_daily WHERE slug = ?',
  )
    .bind(slug)
    .first<{ n: number }>();
  return row?.n ?? 0;
}

describe('sweepOrphans', () => {
  // O caso que o job existe para resolver: o KV apagou a chave sozinho pelo
  // TTL, e sobraram linhas no D1 e um objeto no R2 sem dono.
  it('removes D1 rows and the R2 object of a slug no longer in KV', async () => {
    await seedClicksAndQr('orfao');

    const report = await sweepOrphans(env);

    expect(report.removed).toContain('orfao');
    expect(await clickRows('orfao')).toBe(0);
    expect(await env.QR.get('qr/orfao.svg')).toBeNull();
  });

  it('leaves a live link untouched', async () => {
    await seedClicksAndQr('vivo');
    await seedLink('vivo');

    const report = await sweepOrphans(env);

    expect(report.removed).not.toContain('vivo');
    expect(await clickRows('vivo')).toBe(1);
    expect(await env.QR.get('qr/vivo.svg')).not.toBeNull();
  });

  it('is safe to run twice', async () => {
    await seedClicksAndQr('duasvezes');

    await sweepOrphans(env);
    const second = await sweepOrphans(env);

    expect(second.removed).not.toContain('duasvezes');
  });

  it('does nothing when there is nothing to inspect', async () => {
    await env.DB.prepare('DELETE FROM clicks_daily').run();
    const report = await sweepOrphans(env);
    expect(report).toEqual({ inspected: 0, removed: [] });
  });
});
