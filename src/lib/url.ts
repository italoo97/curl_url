export type UrlRejection =
  | 'not-a-url'
  | 'unsupported-scheme'
  | 'private-host'
  | 'too-long';

const MAX_URL_LENGTH = 2048;

/**
 * Hosts que um encurtador público não deve redirecionar para. Sem isso, o
 * serviço vira um proxy de SSRF: alguém encurta http://169.254.169.254/... e
 * usa o seu domínio como fachada para alcançar rede interna de terceiros.
 */
const BLOCKED_HOSTNAMES = new Set([
  'localhost',
  '127.0.0.1',
  '0.0.0.0',
  '::1',
  '169.254.169.254', // metadata de instância em várias clouds
]);

function isPrivateHost(hostname: string): boolean {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, '');
  if (BLOCKED_HOSTNAMES.has(host)) return true;
  if (host.endsWith('.localhost') || host.endsWith('.internal')) return true;
  if (/^10\./.test(host)) return true;
  if (/^192\.168\./.test(host)) return true;
  if (/^172\.(1[6-9]|2\d|3[01])\./.test(host)) return true;
  return false;
}

export function parseTargetUrl(
  raw: unknown,
): { ok: true; url: string } | { ok: false; reason: UrlRejection } {
  if (typeof raw !== 'string' || raw.trim() === '') {
    return { ok: false, reason: 'not-a-url' };
  }
  if (raw.length > MAX_URL_LENGTH) {
    return { ok: false, reason: 'too-long' };
  }

  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    return { ok: false, reason: 'not-a-url' };
  }

  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    return { ok: false, reason: 'unsupported-scheme' };
  }
  if (isPrivateHost(parsed.hostname)) {
    return { ok: false, reason: 'private-host' };
  }

  return { ok: true, url: parsed.toString() };
}
