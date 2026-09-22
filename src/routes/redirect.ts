import { problem } from '../lib/http.js';
import { isValidSlug } from '../lib/slug.js';
import type { Env, StoredLink } from '../types.js';

export async function redirect(
  slug: string,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  if (!isValidSlug(slug)) {
    return problem(404, 'Link não encontrado.');
  }

  const stored = await env.LINKS.get<StoredLink>(`link:${slug}`, 'json');
  if (stored === null) {
    return problem(404, 'Link não encontrado.');
  }

  if (stored.expiresAt !== null && Date.parse(stored.expiresAt) <= Date.now()) {
    return problem(410, 'Esse link expirou.');
  }

  const coordinator = env.CLICKS.get(env.CLICKS.idFromName(slug));

  // Ao contrário da contagem, esta chamada é aguardada: perder um clique
  // custa uma linha de estatística, mas deixar duas pessoas usarem o mesmo
  // link de uso único é exatamente o bug que a feature existe para impedir.
  // O preço é uma ida ao Durable Object antes do 302 -- pago só aqui.
  if (stored.oneTime && !(await coordinator.claim())) {
    return problem(410, 'Esse link já foi usado.');
  }

  // O clique é contado fora do caminho de resposta: waitUntil deixa o 302
  // sair imediatamente e o incremento terminar depois. Medir não pode
  // custar latência para quem clicou.
  ctx.waitUntil(coordinator.increment());

  // 302 e não 301: um 301 fica em cache no navegador para sempre, e a
  // partir daí o clique nunca mais chega no Worker -- a contagem da Fase 2
  // simplesmente pararia de existir para quem já clicou uma vez.
  return new Response(null, {
    status: 302,
    headers: { location: stored.url, 'cache-control': 'no-store' },
  });
}
