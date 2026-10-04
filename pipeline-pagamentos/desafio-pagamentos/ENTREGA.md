# Desafio Final — Pipeline de pagamentos
Autor: Vando Ramos
Status: PENDENTE DE EXECUÇÃO REAL

## Camada 1 — Fundação
Environment: [ID]; cluster Basic: [ID]; AWS/us-east-1.
Anexar evidencias/00-identidades-acls.txt e listar SAs/ACLs efetivas.

| Identidade | Permissão | Justificativa |
|---|---|---|
| desafio-producer | WRITE/DESCRIBE prefixos desafio- e desafio. | CDC, sem usar conta pessoal |
| desafio-consumer | READ/DESCRIBE tópicos; READ grupo desafio- | consumo, sem escrita |
| identidade Flink | [permissões reais] | leitura, processamento e sinks |
| cdc_user | LOGIN, REPLICATION, CONNECT, USAGE, SELECT | ler WAL e snapshot; sem DML nas tabelas |

## Camada 2 — Contrato
Anexar schemas reais exportados; confirmar namespace, decimal(15,2), PII/PCI.
Compatibilidade: anexar 02-compatibility.txt. BACKWARD: novo campo com default passa; novo campo sem default falha; remoção passa neste caso. FORWARD: remoção de card_id obrigatório falha. Divergência do enunciado explicitada e confirmada com instrutor: [resposta].

## Camada 3 — CDC
Contagens iniciais: [saída real]; seed=42.
Anexar 01-envelopes-cdc.json com key/value completos e topic/partition/offset de c/u/d/null. Tombstone precisa estar identificado como valor Kafka null, não envelope after=null.
Conector: [ID], RUNNING: [evidência], heartbeat=60000; REPLICA IDENTITY FULL: [evidência].

## Camada 4 — Flink
Anexar DDLs, statements, EXPLAIN e status RUNNING reais.
Join temporal: [evidência de conta enriquecendo transação].
Regra: RULE_VELOCITY, cartão [id], transações [ids], intervalo [segundos].
Anexar 03-fraude.json com alerta e as três transações.
Tempo adotado: [negócio/ingestão]; watermark: [config]; idle-timeout: 5s.

## Camada 5 — Operação
Observabilidade: janela [UTC início/fim]; received_bytes pico [valor/unidade/agregação]; grupo [nome], lag máximo [valor]. Anexar responses reais da API, queries sanitizadas e cálculo.
Segurança: tabela acima atualizada com permissões realmente concedidas; evidência de segredos fora do repositório e PII/PCI.
Custos: período [datas], US$ [total medido], créditos [valor se houver]; anexo billing. Alavancas: desligar connector/pool; limitar retenção/volume.

| Trecho | Garantia/configuração efetiva | Risco e recuperação |
|---|---|---|
| Postgres → CDC | [snapshot/WAL/slot] | WAL retido; monitorar slot; recuperar posição |
| CDC → Kafka | [modo de entrega real] | reenvio; validar offsets e chave |
| Kafka → Flink | [checkpoint/estado/config] | restart; reprocessamento; verificar statement |
| Flink → Kafka | [semântica sink efetiva] | validar commits/alertas duplicados |
| Kafka → consumer → SQLite | efeito com chave única antes de commit | crash após efeito causa replay deduplicado |

Elo fraco: consumer e efeitos externos. SQLite e Kafka não compartilham transação; código persiste primeiro, depois commit. Não garante exactly-once em e-mail/API. Para esses efeitos, usar outbox/chave idempotente suportada pelo destino.

## Custo e teardown
Custo total real: US$ [valor]. Não preenchido antes da medição.
Anexar 06-teardown.txt com comandos/IDs, listas reais de connector/statements vazias, remoção do pool e slot inativo/removido. Conclusão em [UTC]. Billing conferido novamente em [UTC].
