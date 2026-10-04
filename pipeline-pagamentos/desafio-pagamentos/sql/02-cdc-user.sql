-- psql recebe cdc_password por variável; não colocar senha no Git.
SELECT format('CREATE ROLE cdc_user LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD %L', :'cdc_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='cdc_user') \gexec
-- Recusa usuário administrativo já existente (por exemplo, criado pelo painel Neon).
DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='cdc_user'
   AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolbypassrls
        OR NOT rolcanlogin OR NOT rolreplication))
   OR pg_has_role('cdc_user','neon_superuser','MEMBER') THEN
   RAISE EXCEPTION 'cdc_user tem atributos incompatíveis. Conclua a migração manual para menor privilégio.';
 END IF;
 IF has_any_column_privilege('cdc_user','public.customers','INSERT')
   OR EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public'
      AND tablename IN ('customers','accounts','cards','merchants','transactions')
      AND (has_table_privilege('cdc_user','public.' || tablename,'INSERT')
        OR has_table_privilege('cdc_user','public.' || tablename,'UPDATE')
        OR has_table_privilege('cdc_user','public.' || tablename,'DELETE')
        OR has_table_privilege('cdc_user','public.' || tablename,'TRUNCATE'))) THEN
   RAISE EXCEPTION 'cdc_user já pode escrever. Revise permissões antes de continuar.';
 END IF;
END $$;
-- Se a role já existe, use a mesma senha ou altere-a de forma privada.
SELECT format('GRANT CONNECT ON DATABASE %I TO cdc_user', current_database()) \gexec
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON customers, accounts, cards, merchants, transactions TO cdc_user;
-- Owner cria publication; cdc_user não precisa criar tabelas/publication.
SELECT 'CREATE PUBLICATION desafio_pub FOR TABLE customers, accounts, cards, merchants, transactions'
WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname='desafio_pub') \gexec
