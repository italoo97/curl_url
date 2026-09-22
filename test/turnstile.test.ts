import { createExecutionContext, env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import worker from '../src/index.js';

const ctx = createExecutionContext();

function create(body: unknown): Promise<Response> {
  return worker.fetch(
    new Request('https://curl.test/api/links', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    }),
    env,
    ctx,
  );
}

describe('anti-bot verification on link creation', () => {
  it('creates the link when the token checks out', async () => {
    const response = await create({ url: 'https://exemplo.com/', turnstileToken: 'ok' });
    expect(response.status).toBe(201);
  });

  // O widget no navegador sozinho não protege nada: qualquer um manda um
  // POST direto no endpoint. É esta verificação que decide.
  it('refuses a request with no token at all', async () => {
    const response = await create({ url: 'https://exemplo.com/' });
    expect(response.status).toBe(403);
  });

  it('refuses a token the siteverify rejects', async () => {
    const response = await create({
      url: 'https://exemplo.com/',
      turnstileToken: 'token-invalido',
    });
    expect(response.status).toBe(403);
  });

  it('refuses a token above the documented 2048-character limit', async () => {
    const response = await create({
      url: 'https://exemplo.com/',
      turnstileToken: 'a'.repeat(2049),
    });
    expect(response.status).toBe(403);
  });

  // Falha fechada: um deploy sem a secret não pode virar porta aberta.
  it('answers 503 instead of letting requests through when unconfigured', async () => {
    const response = await worker.fetch(
      new Request('https://curl.test/api/links', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ url: 'https://exemplo.com/', turnstileToken: 'ok' }),
      }),
      { ...env, TURNSTILE_SECRET_KEY: '' },
      ctx,
    );
    expect(response.status).toBe(503);
  });
});

describe('GET /api/config', () => {
  it('exposes the public sitekey for the front-end', async () => {
    const response = await worker.fetch(
      new Request('https://curl.test/api/config'),
      env,
      ctx,
    );
    const body = (await response.json()) as { turnstileSiteKey: string };

    expect(response.status).toBe(200);
    expect(body.turnstileSiteKey).toBe(env.TURNSTILE_SITE_KEY);
  });
});
