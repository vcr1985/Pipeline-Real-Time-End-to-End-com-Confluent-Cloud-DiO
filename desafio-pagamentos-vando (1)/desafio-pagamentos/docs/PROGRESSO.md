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
