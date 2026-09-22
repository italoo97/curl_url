import { applyD1Migrations, env } from 'cloudflare:test';
import type { D1Migration } from '@cloudflare/vitest-pool-workers';

// TEST_MIGRATIONS não é binding da aplicação: é injetado pelo
// vitest.config.ts só para este setup. O cast fica contido aqui, em vez de
// acrescentar o campo a Cloudflare.Env e fazer o código de produção
// enxergar um binding que não existe em produção.
const { TEST_MIGRATIONS } = env as unknown as { TEST_MIGRATIONS: D1Migration[] };

// As migrations são a fonte da verdade do esquema nos dois lados -- teste e
// produção -- em vez de um CREATE TABLE duplicado aqui que pode divergir.
await applyD1Migrations(env.DB, TEST_MIGRATIONS);
