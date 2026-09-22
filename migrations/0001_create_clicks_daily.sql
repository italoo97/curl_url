-- Agregado por dia, nunca uma linha por clique: o teto de 10 GB por banco D1
-- é rígido e não pode ser aumentado, então guardar evento bruto é um caminho
-- sem volta. A granularidade diária responde tudo que o endpoint de
-- estatísticas precisa.
CREATE TABLE IF NOT EXISTS clicks_daily (
  slug  TEXT    NOT NULL,
  day   TEXT    NOT NULL,  -- YYYY-MM-DD, UTC
  count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (slug, day)
);

CREATE INDEX IF NOT EXISTS idx_clicks_daily_slug ON clicks_daily (slug);
