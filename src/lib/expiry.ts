/** Mínimo aceito pelo TTL nativo do KV. */
export const MIN_EXPIRES_IN_SECONDS = 60;

/** Um ano. Acima disso, o link é permanente. */
export const MAX_EXPIRES_IN_SECONDS = 31_536_000;

export type ExpiryRejection = 'not-an-integer' | 'out-of-range';

export function parseExpiresIn(
  raw: unknown,
): { ok: true; seconds: number | null } | { ok: false; reason: ExpiryRejection } {
  if (raw === undefined || raw === null) {
    return { ok: true, seconds: null };
  }
  if (typeof raw !== 'number' || !Number.isInteger(raw)) {
    return { ok: false, reason: 'not-an-integer' };
  }
  if (raw < MIN_EXPIRES_IN_SECONDS || raw > MAX_EXPIRES_IN_SECONDS) {
    return { ok: false, reason: 'out-of-range' };
  }
  return { ok: true, seconds: raw };
}
