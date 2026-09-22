const SITEVERIFY_URL = 'https://challenges.cloudflare.com/turnstile/v0/siteverify';

/** A doc define este teto para o token. Barramos antes de gastar a chamada. */
const MAX_TOKEN_LENGTH = 2048;

/** Não esperamos indefinidamente pelo siteverify. */
const TIMEOUT_MS = 5_000;

export type TurnstileOutcome =
  | { ok: true }
  | { ok: false; reason: 'missing-token' | 'invalid-token' | 'unavailable' };

interface SiteverifyResponse {
  success: boolean;
  'error-codes'?: string[];
}

/**
 * Valida o token do Turnstile no servidor.
 *
 * O widget no navegador sozinho não protege nada: qualquer um pode mandar
 * uma string arbitrária para o endpoint. É esta chamada que decide, e ela
 * só pode acontecer no backend -- expor a secret no front devolveria o
 * problema para quem se quer barrar.
 */
export async function verifyTurnstile(
  token: unknown,
  secret: string,
  remoteIp: string | null,
): Promise<TurnstileOutcome> {
  if (typeof token !== 'string' || token === '') {
    return { ok: false, reason: 'missing-token' };
  }
  if (token.length > MAX_TOKEN_LENGTH) {
    return { ok: false, reason: 'invalid-token' };
  }

  const body = new FormData();
  body.append('secret', secret);
  body.append('response', token);
  if (remoteIp !== null) body.append('remoteip', remoteIp);

  let response: Response;
  try {
    response = await fetch(SITEVERIFY_URL, {
      method: 'POST',
      body,
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
  } catch {
    // Falha de rede ou timeout não é prova de que o visitante é humano.
    return { ok: false, reason: 'unavailable' };
  }

  if (!response.ok) return { ok: false, reason: 'unavailable' };

  const result = (await response.json()) as SiteverifyResponse;
  return result.success ? { ok: true } : { ok: false, reason: 'invalid-token' };
}
