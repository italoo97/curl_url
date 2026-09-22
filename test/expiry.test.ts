import { createExecutionContext, env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import worker from '../src/index.js';
import type { StoredLink } from '../src/types.js';

const ctx = createExecutionContext();

async function create(body: unknown): Promise<Response> {
  return worker.fetch(
    new Request('https://curl.test/api/links', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ ...(body as object), turnstileToken: 'ok' }),
    }),
    env,
    ctx,
  );
}

describe('POST /api/links with expiresIn', () => {
  it('stores an expiry and reports it', async () => {
    const response = await create({ url: 'https://exemplo.com/', expiresIn: 3600 });
    const body = (await response.json()) as { slug: string; expiresAt: string };

    expect(response.status).toBe(201);
    expect(Date.parse(body.expiresAt)).toBeGreaterThan(Date.now());

    const stored = await env.LINKS.get<StoredLink>(`link:${body.slug}`, 'json');
    expect(stored?.expiresAt).toBe(body.expiresAt);
  });

  it('leaves the link permanent when expiresIn is omitted', async () => {
    const response = await create({ url: 'https://exemplo.com/' });
    const body = (await response.json()) as { expiresAt: string | null };
    expect(body.expiresAt).toBeNull();
  });

  it.each([30, 40_000_000])(
    'refuses %d seconds, outside the allowed range',
    async (value) => {
      const response = await create({ url: 'https://exemplo.com/', expiresIn: value });
      expect(response.status).toBe(422);
    },
  );

  it.each(['3600', 12.5])('refuses %j, which is not an integer', async (value) => {
    const response = await create({ url: 'https://exemplo.com/', expiresIn: value });
    expect(response.status).toBe(422);
  });

  it('returns 410 once the stored expiry is in the past', async () => {
    const slug = 'vencido';
    await env.LINKS.put(
      `link:${slug}`,
      JSON.stringify({
        url: 'https://exemplo.com/',
        createdAt: '2020-01-01T00:00:00.000Z',
        expiresAt: '2020-01-02T00:00:00.000Z',
        oneTime: false,
      } satisfies StoredLink),
    );

    const response = await worker.fetch(
      new Request(`https://curl.test/${slug}`),
      env,
      ctx,
    );
    expect(response.status).toBe(410);
  });
});
