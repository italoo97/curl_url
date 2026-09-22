import { cloudflareTest, readD1Migrations } from '@cloudflare/vitest-pool-workers';
import { defineConfig } from 'vitest/config';

// Os testes rodam dentro do workerd de verdade, com os bindings declarados
// no wrangler.jsonc: o KV, o Durable Object e o D1 de teste têm a mesma API
// que os de produção, com armazenamento isolado por teste. É o equivalente
// ao FakeCalendarClient do medical_agent -- sem rede, determinístico, e
// ainda assim exercitando o contrato real.
export default defineConfig(async () => {
  const migrations = await readD1Migrations('./migrations');

  return {
    plugins: [
      cloudflareTest({
        wrangler: { configPath: './wrangler.jsonc' },
        miniflare: {
          // Não é um binding da aplicação: existe só para o setup aplicar as
          // migrations no D1 de teste.
          bindings: {
            TEST_MIGRATIONS: migrations,
            // Secret de teste oficial do Turnstile. As chamadas ao
            // siteverify são interceptadas pelo outboundService abaixo, então
            // este valor nunca sai da máquina.
            TURNSTILE_SECRET_KEY: '1x0000000000000000000000000000000AA',
          },
          // Nenhum teste toca a rede: toda saída do Worker cai neste
          // serviço. O siteverify do Turnstile responde conforme o token
          // enviado -- qualquer token contendo "invalido" reprova, o resto
          // aprova. Assim cada teste escolhe o desfecho pelo dado que
          // manda, sem registrar mock próprio.
          outboundService: async (request: Request): Promise<Response> => {
            const url = new URL(request.url);
            if (url.hostname !== 'challenges.cloudflare.com') {
              return new Response('rede bloqueada nos testes', { status: 502 });
            }
            const body = await request.text();
            return Response.json({ success: !body.includes('invalido') });
          },
        },
      }),
    ],
    test: { setupFiles: ['./test/setup.ts'] },
  };
});
