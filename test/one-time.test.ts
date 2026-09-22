import { createExecutionContext, env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import worker from '../src/index.js';

const ctx = createExecutionContext();

async function createLink(options: { oneTime?: boolean } = {}): Promise<string> {
  const response = await worker.fetch(
    new Request('https://curl.test/api/links', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        url: 'https://exemplo.com/uso-unico',
        turnstileToken: 'ok',
        ...options,
      }),
    }),
    env,
    ctx,
  );
  return ((await response.json()) as { slug: string }).slug;
}

function open(slug: string): Promise<Response> {
  return worker.fetch(new Request(`https://curl.test/${slug}`), env, ctx);
}

describe('single-use links', () => {
  it('redirects the first time and refuses afterwards', async () => {
    const slug = await createLink({ oneTime: true });

    expect((await open(slug)).status).toBe(302);
    expect((await open(slug)).status).toBe(410);
    expect((await open(slug)).status).toBe(410);
  });

  // O teste que justifica o Durable Object aqui. No KV, dez requisições
  // simultâneas leriam "não usado" antes de qualquer escrita e todas
  // passariam -- que é precisamente a falha que a feature existe para
  // impedir. Exatamente uma pode vencer.
  it('lets exactly one of ten simultaneous requests through', async () => {
    const slug = await createLink({ oneTime: true });

    const responses = await Promise.all(Array.from({ length: 10 }, () => open(slug)));
    const statuses = responses.map((r) => r.status);

    expect(statuses.filter((s) => s === 302)).toHaveLength(1);
    expect(statuses.filter((s) => s === 410)).toHaveLength(9);
  });

  it('does not restrict a normal link', async () => {
    const slug = await createLink();

    expect((await open(slug)).status).toBe(302);
    expect((await open(slug)).status).toBe(302);
  });

  it('reports the flag back on creation', async () => {
    const response = await worker.fetch(
      new Request('https://curl.test/api/links', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          url: 'https://exemplo.com/',
          oneTime: true,
          turnstileToken: 'ok',
        }),
      }),
      env,
      ctx,
    );
    expect(((await response.json()) as { oneTime: boolean }).oneTime).toBe(true);
  });
});
