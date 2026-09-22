import { json, problem } from '../lib/http.js';
import { isValidSlug } from '../lib/slug.js';
import type { Env, StoredLink } from '../types.js';

/** Janela máxima devolvida em `byDay`, para limitar o custo da query. */
const MAX_DAYS = 90;

interface DailyRow {
  day: string;
  count: number;
}

export async function stats(slug: string, env: Env): Promise<Response> {
  if (!isValidSlug(slug)) {
    return problem(404, 'Link não encontrado.');
  }

  const link = await env.LINKS.get<StoredLink>(`link:${slug}`, 'json');
  if (link === null) {
    return problem(404, 'Link não encontrado.');
  }

  // As duas metades da contagem vêm de lugares diferentes, de propósito: o
  // D1 tem o que já foi descarregado, o Durable Object tem o que ainda está
  // acumulando. Somar os dois é o que faz o total ser verdadeiro agora, e
  // não com até dez segundos de atraso.
  const [flushed, pending] = await Promise.all([
    env.DB.prepare(
      `SELECT day, count FROM clicks_daily
       WHERE slug = ?
       ORDER BY day DESC
       LIMIT ?`,
    )
      .bind(slug, MAX_DAYS)
      .all<DailyRow>(),
    env.CLICKS.get(env.CLICKS.idFromName(slug)).pending(),
  ]);

  const byDay = flushed.results;
  const flushedTotal = byDay.reduce((sum, row) => sum + row.count, 0);

  return json({
    slug,
    url: link.url,
    createdAt: link.createdAt,
    total: flushedTotal + pending,
    // Exposto em vez de escondido: `byDay` só enxerga o que já foi para o
    // D1, então quem consome a API sabe exatamente de onde vem a diferença.
    pending,
    byDay,
  });
}
