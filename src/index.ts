import { sweepOrphans } from './jobs/sweep-orphans.js';
import { problem } from './lib/http.js';
import { createLink } from './routes/create-link.js';
import { qr } from './routes/qr.js';
import { redirect } from './routes/redirect.js';
import { stats } from './routes/stats.js';
import type { Env } from './types.js';

// O runtime precisa encontrar a classe exportada pelo entrypoint para
// instanciar o Durable Object declarado no wrangler.jsonc.
export { LinkCoordinator } from './durable-objects/link-coordinator.js';

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === '/api/links') {
      return request.method === 'POST'
        ? createLink(request, env)
        : problem(405, 'Use POST para criar um link.');
    }

    const linkAction = /^\/api\/links\/([^/]+)\/(stats|qr)$/.exec(path);
    if (linkAction?.[1] !== undefined) {
      if (request.method !== 'GET') {
        return problem(405, 'Use GET neste endereço.');
      }
      const slug = linkAction[1];
      return linkAction[2] === 'qr' ? qr(slug, request, env) : stats(slug, env);
    }

    // A sitekey é pública por definição; entregá-la assim evita cravá-la
    // no HTML e permite trocar entre ambientes sem rebuild do front.
    if (path === '/api/config') {
      return Response.json({ turnstileSiteKey: env.TURNSTILE_SITE_KEY });
    }

    if (path === '/api/health') {
      return new Response('ok', { headers: { 'content-type': 'text/plain' } });
    }

    const slug = path.slice(1);
    if (request.method === 'GET' && slug !== '') {
      return redirect(slug, env, ctx);
    }

    return env.ASSETS.fetch(request);
  },

  // Cron Trigger: recolhe o que o TTL do KV deixa para trás em outros
  // serviços. Ver src/jobs/sweep-orphans.ts.
  async scheduled(_controller: ScheduledController, env: Env): Promise<void> {
    const report = await sweepOrphans(env);
    console.log(
      `sweep-orphans: ${report.inspected} inspecionados, ${report.removed.length} removidos`,
    );
  },
} satisfies ExportedHandler<Env>;
