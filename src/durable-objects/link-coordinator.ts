import { DurableObject } from 'cloudflare:workers';
import type { Env } from '../types.js';

/** Espera antes de descarregar o acumulado no D1. */
const FLUSH_DELAY_MS = 10_000;

/** Acima disso, descarrega na hora em vez de esperar o alarme. */
const FLUSH_THRESHOLD = 100;

/**
 * A instância autoritativa de um slug.
 *
 * Existe porque nenhuma das opções mais simples funciona (ver ADR-005):
 * o KV limita a uma escrita por segundo na mesma chave, e uma escrita no D1
 * por clique serializaria -- cada banco D1 processa uma query por vez.
 *
 * O Durable Object é instância única e endereçável por slug, então todas as
 * escritas daquele link passam por aqui, em ordem. Acumulamos na memória
 * durável e descarregamos no D1 em lote, por alarme ou por volume.
 *
 * Não é preciso trancar nada em torno do par ler/zerar: o input gating do
 * runtime segura eventos novos enquanto um evento está aguardando operação
 * de storage, então dois flushes não se atropelam.
 */
export class LinkCoordinator extends DurableObject<Env> {
  /**
   * Reivindica um link de uso único. Devolve true só para o primeiro
   * chamador; todos os outros recebem false.
   *
   * É a operação que o KV não consegue fazer: lá, dois cliques simultâneos
   * leriam "não usado" antes de qualquer escrita e os dois passariam. Aqui o
   * input gating do runtime serializa o par ler/escrever, então "primeiro"
   * tem significado.
   */
  async claim(): Promise<boolean> {
    if (((await this.ctx.storage.get<boolean>('used')) ?? false) === true) {
      return false;
    }
    await this.ctx.storage.put('used', true);
    return true;
  }

  async increment(): Promise<void> {
    const pending = ((await this.ctx.storage.get<number>('pending')) ?? 0) + 1;
    await this.ctx.storage.put('pending', pending);

    if (pending >= FLUSH_THRESHOLD) {
      await this.flush();
      return;
    }

    // Um alarme por janela: se já existe um agendado, ele cobre este clique.
    if ((await this.ctx.storage.getAlarm()) === null) {
      await this.ctx.storage.setAlarm(Date.now() + FLUSH_DELAY_MS);
    }
  }

  /** Quanto ainda não foi para o D1. Usado nos testes e no endpoint de stats. */
  async pending(): Promise<number> {
    return (await this.ctx.storage.get<number>('pending')) ?? 0;
  }

  override async alarm(): Promise<void> {
    await this.flush();
  }

  private async flush(): Promise<void> {
    const pending = (await this.ctx.storage.get<number>('pending')) ?? 0;
    // O nome do DO é o próprio slug (criado via idFromName), então não
    // precisamos guardá-lo duplicado no storage.
    const slug = this.ctx.id.name;
    if (pending === 0 || slug === undefined) return;

    const day = new Date().toISOString().slice(0, 10);
    await this.env.DB.prepare(
      `INSERT INTO clicks_daily (slug, day, count) VALUES (?, ?, ?)
       ON CONFLICT(slug, day) DO UPDATE SET count = count + excluded.count`,
    )
      .bind(slug, day, pending)
      .run();

    await this.ctx.storage.put('pending', 0);
    await this.ctx.storage.deleteAlarm();
  }
}
