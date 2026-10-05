-- Pipeline validado em 05/10/2026.
-- Catalog: desafio-final | Database: desafio-basic.
-- Executar cada statement separadamente no Flink.
-- Os INSERTs sao jobs continuos; evitar iniciar duplicados.
-- Enriquecimento temporal integrado ainda pendente.

CREATE TABLE `desafio-pagamentos.accounts-normalized` (
  id BIGINT NOT NULL,
  customer_id BIGINT NOT NULL,
  status STRING NOT NULL,
  balance DECIMAL(15,2) NOT NULL,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.cleanup-policy' = 'compact'
);

INSERT INTO `desafio-pagamentos.accounts-normalized`
SELECT id, customer_id, status, balance
FROM `desafio-pagamentos.public.accounts`;

CREATE TABLE `desafio-pagamentos.transaction-events` (
  card_id BIGINT NOT NULL,
  id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  merchant_id BIGINT NOT NULL,
  amount DECIMAL(15,2) NOT NULL,
  event_time STRING NOT NULL,
  event_ts TIMESTAMP_LTZ(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
)
DISTRIBUTED BY HASH(card_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.cleanup-policy' = 'delete',
  'kafka.retention.time' = '7 d'
);

INSERT INTO `desafio-pagamentos.transaction-events`
(card_id, id, account_id, merchant_id, amount, event_time, event_ts)
SELECT
  card_id, id, account_id, merchant_id, amount, event_time,
  CAST(
    TO_TIMESTAMP_LTZ(
      event_time,
      'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''',
      'UTC'
    ) AS TIMESTAMP_LTZ(3)
  )
FROM TO_CHANGELOG(
  input => TABLE `desafio-pagamentos.public.transactions`,
  op_mapping => MAP['INSERT', 'I']
);
-- Este fluxo considera insercoes; updates e deletes nao entram.

CREATE TABLE `desafio.fraud.detected` (
  card_id BIGINT,
  primeira_transacao BIGINT,
  ultima_transacao BIGINT,
  account_id BIGINT,
  quantidade BIGINT,
  valor_total DECIMAL(38,2),
  inicio TIMESTAMP_LTZ(3),
  fim TIMESTAMP_LTZ(3)
)
DISTRIBUTED BY HASH(card_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.cleanup-policy' = 'delete',
  'kafka.retention.time' = '7 d'
);

INSERT INTO `desafio.fraud.detected`
SELECT card_id, primeira_transacao, ultima_transacao,
       account_id, quantidade, valor_total, inicio, fim
FROM `desafio-pagamentos.transaction-events`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY event_ts
  MEASURES
    FIRST(T.id) AS primeira_transacao,
    LAST(T.id) AS ultima_transacao,
    LAST(T.account_id) AS account_id,
    COUNT(T.id) AS quantidade,
    SUM(T.amount) AS valor_total,
    FIRST(T.event_ts) AS inicio,
    LAST(T.event_ts) AS fim
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE T AS T.amount > 0
);
