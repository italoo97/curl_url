import { createExecutionContext, env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import worker from '../src/index.js';

const ctx = createExecutionContext();

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

function getQr(slug: string): Promise<Response> {
  return worker.fetch(new Request(`https://curl.test/api/links/${slug}/qr`), env, ctx);
}

describe('GET /api/links/:slug/qr', () => {
  it('generates an SVG on the first request', async () => {
    const slug = await createLink('https://exemplo.com/qr');
    const response = await getQr(slug);

    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toContain('image/svg+xml');
    expect(response.headers.get('x-qr-cache')).toBe('miss');
    expect(await response.text()).toContain('<svg');
  });

  // O ponto da Fase 4: a segunda chamada não gera nada, lê do R2.
  it('serves the cached object on the second request', async () => {
    const slug = await createLink('https://exemplo.com/qr-cache');

    const first = await getQr(slug);
    const second = await getQr(slug);

    expect(first.headers.get('x-qr-cache')).toBe('miss');
    expect(second.headers.get('x-qr-cache')).toBe('hit');
    expect(await second.text()).toBe(await first.text());
  });

  it('writes the object to R2 under a predictable key', async () => {
    const slug = await createLink('https://exemplo.com/qr-chave');
    await getQr(slug);

    const object = await env.QR.get(`qr/${slug}.svg`);
    expect(object).not.toBeNull();
    expect(object?.httpMetadata?.contentType).toContain('image/svg+xml');
  });

  // Se apontasse para o destino final, o clique não passaria pelo Worker e
  // não seria contado -- e trocar o destino exigiria reimprimir o QR.
  it('encodes the short url, not the destination', async () => {
    const slug = await createLink('https://exemplo.com/destino-real');
    const svg = await (await getQr(slug)).text();

    // O payload não aparece legível no SVG, então comparamos com o QR que o
    // mesmo conteúdo produziria.
    const { renderSVG } = await import('uqr');
    expect(svg).toBe(renderSVG(`https://curl.test/${slug}`));
    expect(svg).not.toBe(renderSVG('https://exemplo.com/destino-real'));
  });

  it('marks the object as immutable', async () => {
    const slug = await createLink('https://exemplo.com/qr-imutavel');
    const response = await getQr(slug);
    expect(response.headers.get('cache-control')).toContain('immutable');
  });

  it('returns 404 for an unknown slug', async () => {
    const response = await getQr('naoexiste');
    expect(response.status).toBe(404);
  });
});
