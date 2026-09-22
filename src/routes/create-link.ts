import { parseExpiresIn } from '../lib/expiry.js';
import { json, problem } from '../lib/http.js';
import { generateSlug } from '../lib/slug.js';
import { verifyTurnstile } from '../lib/turnstile.js';
import { parseTargetUrl } from '../lib/url.js';
import type { Env, StoredLink } from '../types.js';

const MAX_SLUG_ATTEMPTS = 5;

export async function createLink(request: Request, env: Env): Promise<Response> {
  // Falha fechada: sem secret configurada o endpoint não atende. A
  // alternativa -- liberar quando falta configuração -- transforma um erro
  // de deploy silencioso em porta aberta.
  if (!env.TURNSTILE_SECRET_KEY) {
    return problem(503, 'Verificação anti-bot indisponível.');
  }

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return problem(400, 'Corpo inválido: envie JSON com o campo "url".');
  }

  const target = parseTargetUrl((body as { url?: unknown } | null)?.url);
  if (!target.ok) {
    const messages: Record<typeof target.reason, string> = {
      'not-a-url': 'Informe uma URL válida no campo "url".',
      'unsupported-scheme': 'Só encurtamos endereços http e https.',
      'private-host': 'Esse endereço aponta para uma rede privada.',
      'too-long': 'URL longa demais (máximo 2048 caracteres).',
    };
    return problem(422, messages[target.reason]);
  }

  const verification = await verifyTurnstile(
    (body as { turnstileToken?: unknown } | null)?.turnstileToken,
    env.TURNSTILE_SECRET_KEY,
    request.headers.get('CF-Connecting-IP'),
  );
  if (!verification.ok) {
    return verification.reason === 'unavailable'
      ? problem(503, 'Não foi possível verificar agora. Tente de novo.')
      : problem(403, 'Verificação anti-bot falhou. Recarregue e tente de novo.');
  }

  const expiry = parseExpiresIn((body as { expiresIn?: unknown } | null)?.expiresIn);
  if (!expiry.ok) {
    return problem(
      422,
      expiry.reason === 'not-an-integer'
        ? '"expiresIn" deve ser um número inteiro de segundos.'
        : '"expiresIn" deve ficar entre 60 segundos e 1 ano.',
    );
  }

  const oneTime = (body as { oneTime?: unknown } | null)?.oneTime === true;

  // Colisão é improvável, mas "improvável" não é "impossível": conferimos
  // antes de gravar e tentamos de novo em vez de sobrescrever um link de
  // outra pessoa silenciosamente.
  for (let attempt = 0; attempt < MAX_SLUG_ATTEMPTS; attempt++) {
    const slug = generateSlug();
    const existing = await env.LINKS.get(`link:${slug}`);
    if (existing !== null) continue;

    const expiresAt =
      expiry.seconds === null
        ? null
        : new Date(Date.now() + expiry.seconds * 1000).toISOString();

    const stored: StoredLink = {
      url: target.url,
      createdAt: new Date().toISOString(),
      expiresAt,
      oneTime,
    };

    // O TTL nativo do KV apaga a chave sozinho no vencimento: não há cron
    // varrendo links expirados, porque a plataforma já faz isso. O campo
    // expiresAt fica no valor apenas para a API poder informá-lo e para o
    // redirect devolver 410 na janela entre o vencimento e a remoção real.
    await env.LINKS.put(
      `link:${slug}`,
      JSON.stringify(stored),
      expiry.seconds === null ? {} : { expirationTtl: expiry.seconds },
    );

    const shortUrl = new URL(`/${slug}`, request.url).toString();
    return json({ slug, url: target.url, shortUrl, expiresAt, oneTime }, 201);
  }

  return problem(503, 'Não foi possível gerar um slug livre. Tente de novo.');
}
