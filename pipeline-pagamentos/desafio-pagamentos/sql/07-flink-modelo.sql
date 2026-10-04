-- MODELO: adaptar depois de SHOW CREATE TABLE nos tópicos inferidos.
-- Não execute antes de conferir before/after, PK, DECIMAL e atributo temporal.
SET 'sql.tables.scan.idle-timeout' = '5s';
-- Contrato necessário:
-- desafio.accounts: id PK NOT ENFORCED; customer_id, status, balance; $rowtime.
-- desafio.transactions: id PK NOT ENFORCED; account_id, card_id, merchant_id,
-- amount DECIMAL(15,2), event_time TIMESTAMP_LTZ(3), $rowtime.
-- transactions_insert: APPEND, exclusivamente envelopes op='c' (e op='r'
-- apenas se houver estratégia explícita para snapshot). Preservar rowtime.
-- Não aplique MATCH_RECOGNIZE sobre a tabela CDC upsert diretamente.

-- Temporal join: publicar em sink APPEND quando a entrada transactions_insert
-- for APPEND e o planner confirmar o changelog do resultado.
INSERT INTO `desafio.payments.enriched`
SELECT t.id, t.card_id, t.amount, a.customer_id, a.status, t.`$rowtime`
FROM transactions_insert AS t
JOIN `desafio.accounts` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
ON t.account_id = a.id;

-- O DDL dos sinks deve casar exatamente com colunas/tipos deste SELECT.
INSERT INTO `desafio.fraud.detected`
SELECT card_id, first_id, last_id, total, first_time, last_time, 'RULE_VELOCITY'
FROM `desafio.payments.enriched`
MATCH_RECOGNIZE (
 PARTITION BY card_id
 ORDER BY `$rowtime`
 MEASURES FIRST(T.id) AS first_id, LAST(T.id) AS last_id,
 COUNT(T.id) AS total, FIRST(T.`$rowtime`) AS first_time,
 LAST(T.`$rowtime`) AS last_time
 ONE ROW PER MATCH
 AFTER MATCH SKIP PAST LAST ROW
 PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
 DEFINE T AS T.amount > 0
);
-- ORDER BY $rowtime mede tempo de ingestão. Para tempo de negócio, definir
-- event_time como atributo de rowtime com WATERMARK nas tabelas apropriadas.
-- Explicar essa escolha e atrasos na entrega.
