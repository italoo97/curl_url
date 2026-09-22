import { createExecutionContext, env, waitOnExecutionContext } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import worker from '../src/index.js';

const ctx = createExecutionContext();

function post(body: unknown): Request {
  return new Request('https://curl.test/api/links', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ...(body as object), turnstileToken: 'ok' }),
  });
}

async function createLink(url: string) {
  const response = await worker.fetch(post({ url }), env, ctx);
  return {
    response,
    body: (await response.json()) as { slug: string; shortUrl: string },
  };
}

describe('POST /api/links', () => {
  it('creates a link and returns its slug', async () => {
    const { response, body } = await createLink('https://exemplo.com/destino');

    expect(response.status).toBe(201);
    expect(body.slug).toMatch(/^[a-zA-Z0-9]{7}$/);
    expect(body.shortUrl).toBe(`https://curl.test/${body.slug}`);
  });

  it('refuses a private address', async () => {
    const { response } = await createLink('http://127.0.0.1/admin');
    expect(response.status).toBe(422);
  });

  it('refuses a method other than POST', async () => {
    const request = new Request('https://curl.test/api/links', { method: 'GET' });
    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(405);
  });
});

describe('GET /:slug', () => {
  it('redirects to the stored url', async () => {
    const { body } = await createLink('https://exemplo.com/destino');

    const request = new Request(`https://curl.test/${body.slug}`);
    const response = await worker.fetch(request, env, ctx);

    expect(response.status).toBe(302);
    expect(response.headers.get('location')).toBe('https://exemplo.com/destino');
    // 301 ficaria em cache no navegador e o clique nunca mais chegaria aqui.
    expect(response.headers.get('cache-control')).toBe('no-store');
  });

  it('returns 404 for an unknown slug', async () => {
    const response = await worker.fetch(
      new Request('https://curl.test/naoexiste'),
      env,
      ctx,
    );
    expect(response.status).toBe(404);
  });

  it('returns 410 for an expired link', async () => {
    const slug = 'expirad';
    await env.LINKS.put(
      `link:${slug}`,
      JSON.stringify({
        url: 'https://exemplo.com/',
        createdAt: '2020-01-01T00:00:00Z',
        expiresAt: '2020-01-02T00:00:00Z',
        oneTime: false,
      }),
    );

    const response = await worker.fetch(
      new Request(`https://curl.test/${slug}`),
      env,
      ctx,
    );
    expect(response.status).toBe(410);
  });
});

describe('click counting', () => {
  it('counts the click without delaying the redirect', async () => {
    const { body } = await createLink('https://exemplo.com/contado');

    await worker.fetch(new Request(`https://curl.test/${body.slug}`), env, ctx);
    await waitOnExecutionContext(ctx);

    const counter = env.CLICKS.get(env.CLICKS.idFromName(body.slug));
    expect(await counter.pending()).toBe(1);
  });

  // É este teste que justifica o Durable Object existir. No KV o limite é de
  // uma escrita por segundo na mesma chave: vinte cliques simultâneos
  // perderiam contagem silenciosamente. Aqui nenhum se perde.
  it('loses no clicks when twenty arrive at once', async () => {
    const { body } = await createLink('https://exemplo.com/viral');

    const burst = Array.from({ length: 20 }, () =>
      worker.fetch(new Request(`https://curl.test/${body.slug}`), env, ctx),
    );
    const responses = await Promise.all(burst);
    await waitOnExecutionContext(ctx);

    expect(responses.every((r) => r.status === 302)).toBe(true);
    const counter = env.CLICKS.get(env.CLICKS.idFromName(body.slug));
    expect(await counter.pending()).toBe(20);
  });

  it('does not count a click on an unknown slug', async () => {
    await worker.fetch(new Request('https://curl.test/naoexiste'), env, ctx);
    await waitOnExecutionContext(ctx);

    const counter = env.CLICKS.get(env.CLICKS.idFromName('naoexiste'));
    expect(await counter.pending()).toBe(0);
  });
});
