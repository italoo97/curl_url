/**
 * Bindings do Worker.
 *
 * Declarados dentro de `Cloudflare.Env` (e não como uma interface solta)
 * porque é esse o tipo que o runtime e o ambiente de testes já usam: assim
 * o `env` importado de 'cloudflare:test' enxerga os mesmos bindings, sem
 * cast e sem duas definições que podem divergir.
 */
declare global {
  namespace Cloudflare {
    interface Env {
      /** Mapeamento slug -> link. Ver ADR-004. */
      LINKS: KVNamespace;
      /** Front estático servido pelo próprio Worker. Ver ADR-002. */
      ASSETS: Fetcher;
      /** Um contador por slug, absorvendo escrita concorrente. Ver ADR-005. */
      CLICKS: DurableObjectNamespace<
        import('./durable-objects/link-coordinator.js').LinkCoordinator
      >;
      /** Agregado de cliques, consultável. Ver ADR-006. */
      DB: D1Database;
      /** QR codes gerados sob demanda e cacheados. Ver ADR-007. */
      QR: R2Bucket;
      /** Sitekey pública do Turnstile, entregue ao front por /api/config. */
      TURNSTILE_SITE_KEY: string;
      /** Secret do Turnstile. `wrangler secret put` em produção. */
      TURNSTILE_SECRET_KEY: string;
    }
  }
}

export type Env = Cloudflare.Env;

/** O que guardamos no KV sob a chave `link:<slug>`. */
export interface StoredLink {
  url: string;
  createdAt: string;
  expiresAt: string | null;
  oneTime: boolean;
}
