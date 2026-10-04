# Desafio Final — Pipeline de pagamentos
**Autor: Vando Ramos**

Base de implementação para IBM/DIO. Status: em desenvolvimento. Banco Neon preparado na sessão guiada; pipeline cloud ainda NÃO executado nem validado ponta a ponta. A entrega só fica concluída após capturar as evidências reais e completar o provisionamento automatizado.

## Progresso da sessão
As cinco tabelas foram criadas e as contagens conferidas: 200 clientes, 240 contas, 320 cartões, 60 comerciantes e 2000 transações. A publicação `desafio_pub` foi conferida com cinco tabelas. Criamos `cdc_restrito` por SQL com REPLICATION e concedemos SELECT nas cinco tabelas. A consulta de verificação das permissões ainda está pendente, assim como login, senha e adoção do nome final `cdc_user`.

Veja [docs/PROGRESSO.md](docs/PROGRESSO.md) para retomar do ponto correto. As confirmações são da sessão guiada; as evidências exportadas devem ser anexadas antes da entrega final.

## Arquitetura e decisões
Neon Postgres → PostgresCdcSourceV2 → Kafka Avro (envelopes completos) → normalização Flink → temporal join contas → fluxo append de pagamentos → MATCH_RECOGNIZE → tópico de alertas → consumer SQLite.

Não guardar PAN/CVV. Todos os dados são sintéticos. Token de cartão é rotulado PCI por cautela; nome/e-mail são PII. Tags não criptografam nem bloqueiam acesso por si só.

### Três ajustes importantes ao exemplo
1. Em Avro BACKWARD, um novo leitor pode ignorar um campo antigo removido. A remoção isolada de campo obrigatório pode passar. O teste negativo BACKWARD aqui adiciona campo sem default; a remoção é rejeitada em FORWARD. Apresente as duas políticas sem falsificar saída e peça ao instrutor confirmação dessa interpretação.
2. `desafio-*` deve ser entendido como **prefixo `desafio-`**, não curinga literal. Esse prefixo NÃO cobre `desafio.fraud.detected`. Criar ACL adicional com prefixo `desafio.` (ou padronizar nomes, se o professor permitir).
3. CDC registra histórico de mudanças; Flink upsert interpreta estado. MATCH_RECOGNIZE precisa append-only. Não basta trocar o nome do tópico para torná-lo append.

## Primeiro passo — preparar contas e ferramentas
- Conta Confluent Cloud com permissão para provisionar environment, cluster, Schema Registry, Connect e Flink; confirmar créditos/limites do bootcamp.
- Projeto/branch Neon exclusivo. Ativar logical replication e verificar `SHOW wal_level` = `logical`. Usar hostname direto, sem `-pooler`.
- Ubuntu: Bash, Python 3, cliente `psql`, CLI Confluent instalada segundo a documentação oficial. Rodar `confluent login` e `confluent version`.
- Flink compute pool na mesma região AWS us-east-1. Registrar ID, capacidade e identidade própria para Flink com acessos necessários; não usar chave pessoal em jobs.
- Nunca enviar .env ou senhas no chat/Git. Preencher tudo localmente.

## Ordem de execução
1. Criar environment `desafio-final` e cluster Basic `desafio-basic`, AWS/us-east-1; habilitar Schema Registry. Anotar IDs.
2. Criar SAs `desafio-producer`, `desafio-consumer` e identidade Flink. Criar chaves Kafka/SR separadas por recurso. A chave Kafka não autentica automaticamente no SR.
3. Configurar ACLs abaixo e permissões específicas do connector conforme documentação; pré-criar tópicos se evitar CREATE amplo. Registrar permissões adicionais exigidas pelo serviço em vez de escondê-las.
4. Copiar `.env.example` para `.env`, preencher e proteger com `chmod 600 .env`.
5. Em banco novo e exclusivo, rodar `chmod +x setup.sh teardown.sh` e `./setup.sh`. Verificar 200/240/320/60/2000. setup prepara banco/config; NÃO cria automaticamente recursos cloud nem statements.
6. Criar conector com `confluent connect cluster create --config-file .runtime/cdc.json` no contexto do cluster correto. Arquivo renderizado contém secrets: manter privado; apagá-lo após uso.
7. Esperar conector RUNNING e snapshot concluído. Snapshot normalmente gera `op=r`; isso NÃO substitui evidência `op=c`.
8. Capturar envelopes completos, chave, tópico, partição e offset. Executar `sql/05-demo-cdc.sql`. A exclusão deve aparecer como `op=d`, seguida de mensagem com valor Kafka null (tombstone). null no campo `after` não é o tombstone.
9. Rodar testes de contrato: `set -a; source .env; set +a` e `python3 scripts/compatibility.py`. Salvar saída em evidencias/02-compatibility.txt. Subject de teste isolado, sem afetar contratos Debezium.
10. No Flink, salvar `SHOW CREATE TABLE` de contas e transações. Completar normalização e DDL antes de executar `sql/07-flink-modelo.sql` (detalhes abaixo).
11. Com jobs RUNNING, rodar `sql/06-trigger-fraud.sql`. Confirmar alerta para IDs 900001/900002/900003, cartão 1; publicar evento posterior para avançar watermark, conforme comentários SQL.
12. Instalar requirements em venv, carregar .env e executar `python3 consumer.py`. Interromper/reiniciar com mesmo grupo; demonstrar que efeito SQLite não duplica para a mesma chave de negócio.
13. Capturar métricas, billing e executar teardown. Só declarar conclusão quando todas as evidências existirem.

## ACLs iniciais (CLI atual: confira --help)
```bash
confluent iam service-account create desafio-producer --description 'CDC pagamentos'
confluent iam service-account create desafio-consumer --description 'Consumo alertas'
# Substituir sa-PRODUCER e sa-CONSUMER pelos IDs retornados.
confluent kafka acl create --allow --service-account sa-PRODUCER --operations WRITE,DESCRIBE --topic desafio- --prefix
confluent kafka acl create --allow --service-account sa-PRODUCER --operations WRITE,DESCRIBE --topic desafio. --prefix
confluent kafka acl create --allow --service-account sa-CONSUMER --operations READ,DESCRIBE --topic desafio- --prefix
confluent kafka acl create --allow --service-account sa-CONSUMER --operations READ,DESCRIBE --topic desafio. --prefix
confluent kafka acl create --allow --service-account sa-CONSUMER --operations READ --consumer-group desafio- --prefix
confluent iam service-account list
confluent kafka acl list
```
Atribuir no SR permissões de registro ao writer e leitura ao consumer, conforme mecanismo disponível na conta. Connector pode precisar criar/descrever tópicos internos e outras permissões documentadas. Limitar ao conjunto necessário; registrar exceções. Consumer não precisa WRITE.

## Contratos Avro e namespace
Os schemas em schemas/ validam o contrato de Payment no namespace `com.bootcamp.payments`, com amount bytes decimal(15,2). Eles não são schemas dos envelopes Debezium e não devem ser registrados por cima deles.

**Pendência de integração:** exportar schemas gerados pelo connector para as cinco tabelas, chaves e envelopes. Conferir mapeamento NUMERIC→decimal(15,2). Aplicar namespace `com.bootcamp.payments.*` aos contratos normalizados/sinks e tags PII/PCI pelo recurso de governança disponível. Se a exigência também se aplicar aos envelopes de origem, configurar/validar transformação suportada de nomes antes de declarar conformidade; não presumir que namespace customizado é gerado automaticamente. Registrar os schemas efetivamente usados em schemas/exportados/.

## Integração Flink que precisa ser finalizada na conta
O arquivo SQL é um modelo deliberadamente marcado. Os tipos de uma tabela inferida dependem dos schemas Kafka realmente gerados.

- Guardar envelope raw completo para evidência; não habilitar after-state-only no connector principal.
- Criar leitura **raw append** dos envelopes para filtrar `op='c'` e extrair `after`. Se o Flink inferir Debezium como upsert, criar uma tabela de leitura alternativa com formato Avro genérico suportado, tipos dos envelopes e atributos de tempo verificados. `WHERE op='c'` só funciona se `op` existir nessa leitura raw.
- Criar tabela normalizada de contas com PK id NOT ENFORCED, changelog/upsert e atributo temporal; propagar snapshots/updates/deletes corretamente. Não perder DELETE.
- Criar tabela normalizada de transações com PK id para satisfazer contrato e uma trilha append de INSERTs para CEP. Declaração de PK deve refletir chave Kafka real; não inventar unicidade.
- DDL de sinks: enriquecido tem id, card_id, amount, customer_id, status e rowtime; fraude tem card_id, first_id, last_id, total, first_time, last_time e rule. Confirmar tipos com DESCRIBE antes do INSERT.
- Validar join temporal com EXPLAIN, watermark nos dois lados e idle-timeout 5s em cada statement aplicável.
- SQL modelo usa `$rowtime` (tempo Kafka/ingestão); para requisito baseado no horário de negócio, usar event_time e WATERMARK explicitamente. Documentar atrasos, ordem e tolerância escolhidos.
- Não aplicar CEP sobre changelog de updates/deletes; capturas CDC desses eventos continuam na camada raw.

## Operação e evidências
Use ENTREGA.md como relatório. Nunca substituir pendência por dado inventado.
Métricas: consultar catálogo Metrics API e construir queries com nomes/labels atuais de received_bytes e consumer lag. Credencial Metrics/Cloud separada, sem imprimir Authorization. Salvar request sanitizado, response e janela UTC. Pico de bytes depende da agregação (taxa vs total); identificar unidade. Lag por grupo pode usar endpoint de lag dedicado, conforme disponibilidade.
Custos: consultar `confluent billing cost list --help`, selecionar período real e salvar saída; registrar data/hora, itens e soma. Conferir billing posterior por atraso de processamento. Créditos não significam consumo zero.
Alavancas: remover connector/pool assim que acabar; reduzir retenção/volume de teste. Não alegar economia sem comparar valores medidos.
Teardown: excluir statements e connector por IDs registrados; verificar listas no environment exclusivo. Remover pool Flink para cessar cobrança; depois chaves, SAs, schemas, tópicos, cluster/environment exclusivos e slot/publication Neon. O script auxilia, mas não remove tudo automaticamente. `None found` só deve constar quando realmente retornado. Schema Registry/neon podem exigir limpeza adicional.

## Critério de conclusão
Este pacote é a base, não uma entrega final já executada. Ainda faltam: provisionamento end-to-end por setup.sh; DDL/normalização Flink validados; schemas/tags efetivos; captura de envelopes e tombstone; alerta real; métricas e billing; teardown completo automatizado e evidenciado. Após primeira execução, incorporar IDs/configs parametrizados e validadores ao setup para atingir reproduzibilidade exigida.

## Usuário CDC no Neon
Criar usuários pelo painel concede associação administrativa `neon_superuser`. Para menor privilégio, criar pelo SQL. O setup recusa um `cdc_user` existente com permissões amplas, em vez de reutilizá-lo silenciosamente. No banco já preparado na sessão, concluir a migração de `cdc_restrito` para o nome final antes de executar o setup; não recriar as tabelas nem recarregar o seed.

## Fontes oficiais
- https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/cc-postgresql-cdc-source-v2-debezium.html
- https://developer.confluent.io/courses/schema-registry/schema-compatibility/
- https://docs.confluent.io/cloud/current/flink/reference/queries/joins.html
- https://docs.confluent.io/cloud/current/flink/reference/queries/match_recognize.html
- https://docs.confluent.io/cloud/current/flink/concepts/dynamic-tables.html
- https://neon.com/blog/stream-data-from-neon-to-external-data-sources-via-logical-replication
