import type { Env } from '../types.js';

/** Teto por execução, para o job não estourar o limite de tempo do Worker. */
const MAX_SLUGS_PER_RUN = 500;

export interface SweepReport {
  inspected: number;
  removed: string[];
}

/**
 * Remove o que sobra de um link que já expirou.
 *
 * O link em si não é problema nosso: o KV tem TTL nativo e apaga a chave
 * sozinho (ver `expirationTtl` em create-link). Reimplementar isso num cron
 * seria reescrever o que a plataforma já faz.
 *
 * O que o KV não faz é limpar o que ficou pendurado em OUTROS serviços: as
 * linhas de `clicks_daily` no D1 e o objeto `qr/<slug>.svg` no R2 continuam
 * lá para sempre, referenciando um slug que não existe mais. É esse órfão
 * que este job recolhe.
 *
 * A varredura parte do D1 (quem tem clique registrado) e confere cada slug
 * contra o KV. Para volume grande, o certo seria registrar a remoção em vez
 * de varrer -- está anotado como limitação conhecida no README.
 */
export async function sweepOrphans(env: Env): Promise<SweepReport> {
  const { results } = await env.DB.prepare(
    'SELECT DISTINCT slug FROM clicks_daily LIMIT ?',
  )
    .bind(MAX_SLUGS_PER_RUN)
    .all<{ slug: string }>();

  const removed: string[] = [];

  for (const { slug } of results) {
    // A chave sumiu do KV: ou o TTL venceu, ou o link foi removido.
    if ((await env.LINKS.get(`link:${slug}`)) !== null) continue;

    await Promise.all([
      env.DB.prepare('DELETE FROM clicks_daily WHERE slug = ?').bind(slug).run(),
      env.QR.delete(`qr/${slug}.svg`),
    ]);
    removed.push(slug);
  }

  return { inspected: results.length, removed };
}
