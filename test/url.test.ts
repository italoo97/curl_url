import { describe, expect, it } from 'vitest';
import { parseTargetUrl } from '../src/lib/url.js';

describe('parseTargetUrl', () => {
  it('accepts a normal https url', () => {
    const result = parseTargetUrl('https://exemplo.com/a?b=1');
    expect(result).toEqual({ ok: true, url: 'https://exemplo.com/a?b=1' });
  });

  it.each([
    ['javascript:alert(1)', 'unsupported-scheme'],
    ['ftp://exemplo.com', 'unsupported-scheme'],
    ['nao é url', 'not-a-url'],
    ['', 'not-a-url'],
  ])('rejects %j as %s', (input, reason) => {
    expect(parseTargetUrl(input)).toEqual({ ok: false, reason });
  });

  // Sem isso o encurtador vira fachada de SSRF: alguém encurta um endereço
  // interno e usa o nosso domínio para alcançá-lo.
  it.each([
    'http://localhost:8080/admin',
    'http://127.0.0.1/',
    'http://169.254.169.254/latest/meta-data/',
    'http://10.0.0.5/',
    'http://192.168.1.1/',
    'http://172.16.0.1/',
  ])('rejects the private address %s', (input) => {
    expect(parseTargetUrl(input)).toEqual({ ok: false, reason: 'private-host' });
  });

  it('rejects a url longer than the limit', () => {
    const long = `https://exemplo.com/${'a'.repeat(2100)}`;
    expect(parseTargetUrl(long)).toEqual({ ok: false, reason: 'too-long' });
  });
});
