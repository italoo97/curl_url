import { renderSVG } from 'uqr';
import { problem } from '../lib/http.js';
import { isValidSlug } from '../lib/slug.js';
import type { Env, StoredLink } from '../types.js';

/**
 * SVG e não PNG.
 *
 * Um QR Code é vetorial por natureza -- uma grade de quadrados. Rasterizar
 * exigiria embarcar um codificador PNG no Worker para produzir um arquivo
 * maior, que perde qualidade ao ser ampliado e ainda precisaria de uma
 * decisão de resolução. O SVG é uma string, escala em qualquer tamanho e
 * imprime bem.
 */
const CONTENT_TYPE = 'image/svg+xml; charset=utf-8';

/** O slug nunca muda, então o QR daquele slug também não. Ver ADR-007. */
const CACHE_CONTROL = 'public, max-age=31536000, immutable';

function objectKey(slug: string): string {
  return `qr/${slug}.svg`;
}

export async function qr(slug: string, request: Request, env: Env): Promise<Response> {
  if (!isValidSlug(slug)) {
    return problem(404, 'Link não encontrado.');
  }

  const link = await env.LINKS.get<StoredLink>(`link:${slug}`, 'json');
  if (link === null) {
    return problem(404, 'Link não encontrado.');
  }

  // Geração sob demanda: a maioria dos links encurtados nunca é
  // compartilhada por QR. Gerar na criação encheria o bucket de arquivos que
  // ninguém pede. O primeiro acesso paga a geração; os seguintes leem do R2.
  const cached = await env.QR.get(objectKey(slug));
  if (cached !== null) {
    return new Response(cached.body, {
      headers: {
        'content-type': CONTENT_TYPE,
        'cache-control': CACHE_CONTROL,
        'x-qr-cache': 'hit',
      },
    });
  }

  // O QR aponta para a URL curta, não para o destino: é o encurtador que
  // conta o clique e o que permite trocar o destino sem reimprimir nada.
  const shortUrl = new URL(`/${slug}`, request.url).toString();
  const svg = renderSVG(shortUrl);

  await env.QR.put(objectKey(slug), svg, {
    httpMetadata: { contentType: CONTENT_TYPE, cacheControl: CACHE_CONTROL },
  });

  return new Response(svg, {
    headers: {
      'content-type': CONTENT_TYPE,
      'cache-control': CACHE_CONTROL,
      'x-qr-cache': 'miss',
    },
  });
}
