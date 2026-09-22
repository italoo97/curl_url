const ALPHABET = 'abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789';

export const SLUG_LENGTH = 7;

/**
 * Gera um slug aleatório com alfabeto sem caracteres ambíguos (sem l, I, 1,
 * 0, O), para que alguém consiga ditar um link por telefone sem errar.
 *
 * Usa crypto.getRandomValues, não Math.random: o slug é o único segredo que
 * protege um link não listado, então precisa ser imprevisível.
 *
 * O módulo introduz um viés desprezível (256 % 57), aceito conscientemente:
 * com 57^7 combinações, a colisão prática é dominada pelo volume, não pelo
 * viés. A verificação de colisão fica em quem grava.
 */
export function generateSlug(length: number = SLUG_LENGTH): string {
  const bytes = crypto.getRandomValues(new Uint8Array(length));
  let slug = '';
  for (const byte of bytes) {
    slug += ALPHABET[byte % ALPHABET.length];
  }
  return slug;
}

const SLUG_PATTERN = new RegExp(`^[${ALPHABET}]{3,32}$`);

export function isValidSlug(candidate: string): boolean {
  return SLUG_PATTERN.test(candidate);
}
