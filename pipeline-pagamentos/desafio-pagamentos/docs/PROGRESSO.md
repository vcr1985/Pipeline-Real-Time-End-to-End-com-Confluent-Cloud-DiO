# Ponto de retomada — Vando Ramos

Registro da sessão de 03/10/2026, baseado nas confirmações e capturas compartilhadas pelo autor.

## Concluído no Neon
- Projeto: Pipeline em tempo real; banco: neondb; região: AWS Ohio (us-east-2).
- Replicação lógica habilitada, wal_level conferido como logical.
- Cinco tabelas criadas e REPLICA IDENTITY FULL executado.
- Seed carregado: customers 200, accounts 240, cards 320, merchants 60, transactions 2000.
- Publicação desafio_pub conferida com cinco tabelas.
- cdc_user criado pelo painel, com associação neon_superuser; consulta confirmou permissão INSERT. Esse usuário ainda NÃO satisfaz menor privilégio.
- Tentativa de revogar neon_superuser recusada; sessão sem transação aberta após ROLLBACK.
- cdc_restrito criado por SQL com NOLOGIN, REPLICATION, NOSUPERUSER, NOCREATEDB e NOCREATEROLE.
- CONNECT, USAGE e SELECT nas cinco tabelas concedidos a cdc_restrito.

## Próximo passo
Executar sql/09-verify-cdc-restrito.sql. Esperado: cinco linhas com leitura=true e INSERT/UPDATE/DELETE=false; replicação=true e atributos administrativos=false; nenhum grupo administrativo.
Depois configurar login e senha privada e concluir a substituição pelo nome cdc_user, verificando dependências do usuário anterior. Não excluir usuários nem alterar senhas automaticamente nesta etapa.

## Pendências do desafio
- Confirmar ativação da conta Confluent, créditos e criação do environment/cluster Basic AWS us-east-1.
- Identidades, ACLs, Schema Registry e conector CDC.
- Contratos efetivos, tags, Flink validado e consumidor em execução.
- Evidências reais de CDC, fraude, métricas, custos e teardown.
- Automatização integral de setup.sh e teardown.sh.

A região Ohio pertence ao projeto Neon atual; o enunciado pede AWS us-east-1 para o cluster Confluent. Não declarar as duas regiões como iguais.
Senhas, arquivos .env, strings de conexão e configurações renderizadas permanecem privados.

## Atualização — 05/10/2026
- Confluent: environment desafio-final; cluster Basic desafio-basic, AWS us-east-1.
- CDC: eventos de criação e atualização recebidos nos tópicos.
- Corrigidas as permissões do tópico heartbeat do conector.
- Flink: normalização de contas, fluxo de transações e detecção de três transações positivas em até 60 segundos funcionando.
- Regra de fraude usa event_ts com precisão de milissegundos e watermark de 5 segundos.
- Consumidor Python: autenticação Kafka e leitura Avro funcionando.
- Schema Registry: DeveloperRead nos subjects desafio.fraud.detected-key e desafio.fraud.detected-value.
- card_id recuperado da chave Avro; demais campos recuperados do valor.
- SQLite: alertas persistidos antes do commit do offset.
- Reinício do consumidor não repetiu o alerta já processado.
- Testes positivos: 900101–900103 e 900201–900203; três transações, total 60.00 em cada alerta.
- Teste negativo: duas transações positivas do cartão 2 não geraram alerta durante a observação.
- SQLite confirmou duas chaves de alerta distintas, sem duplicação dessas chaves.

### Pendências atuais
- Atualizar SQL e README para refletir a implementação executada.
- Conferir novamente os privilégios finais do usuário CDC.
- Concluir contratos, compatibilidade de schemas e tags.
- Validar enriquecimento temporal integrado à saída final.
- Registrar métricas, custos e procedimento de teardown.
- Conferir scripts de setup e teardown.

As pendências do registro de 03/10 são históricas; usar esta atualização como ponto de retomada.

### Verificação do usuário CDC — 05/10/2026
- cdc_user: LOGIN e REPLICATION habilitados.
- SUPERUSER, CREATEDB, CREATEROLE e BYPASSRLS desabilitados.
- SELECT permitido nas cinco tabelas; INSERT, UPDATE e DELETE negados.
- CONNECT em neondb e USAGE em public permitidos; CREATE em public negado.
- Consulta de associação a grupos executada; nenhuma linha informada.
- sql/09-verify-cdc-restrito.sql atualizado para consultar cdc_user.

### Compatibilidade Avro — 05/10/2026
- Subject de teste: desafio-contract-payment-value; schema v1 ID 100019.
- Tag PCI criada; schema v1 registrado com PCI no campo card_id.
- BACKWARD: adicionar channel com default passou; sem default foi rejeitado.
- Remover card_id passou em BACKWARD e foi rejeitado em FORWARD.
- Política final restaurada para BACKWARD.
- Evidência: docs/evidencias/compatibilidade.txt.
- Schemas do CDC não foram alterados por esse teste.

## Governança — 05/10/2026
- Schema customers: campos name e email com tag PII; Schema ID 100020.
- Schema cards: campo token com tag PCI, confirmado no JSON salvo.
- Conector desafio-postgres-cdc conferido após as alterações, sem erro informado.
- Tags classificam os dados; não criptografam nem bloqueiam acesso por si só.

## Retomada — 06/10/2026
- Lote 900501–900504: uma ocorrência por ID em transaction-events e payments.enriched.
- Detector passou a ler payments.enriched, com corte em event_ts >= 2026-10-06 04:41:00 UTC para excluir o histórico contaminado.
- Consumidor recebeu o alerta 900501–900503 no offset 11: três transações, total 60.00, intervalo de 20 segundos.
- SQLite confirmou uma linha para a chave desse alerta.
- Novas credenciais Kafka e Schema Registry testadas com sucesso.
- Duplicações históricas continuam armazenadas; o corte temporal não as remove nem impede duplicação por futuras reexecuções.
- Pendente: atualizar SQL versionado com o detector atual e documentar a recuperação.
