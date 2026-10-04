-- Consulta somente leitura; execute como owner no Editor SQL.
SELECT tablename AS tabela,
 has_table_privilege('cdc_restrito', 'public.' || tablename, 'SELECT') AS pode_ler,
 has_table_privilege('cdc_restrito', 'public.' || tablename, 'INSERT') AS pode_inserir,
 has_table_privilege('cdc_restrito', 'public.' || tablename, 'UPDATE') AS pode_alterar,
 has_table_privilege('cdc_restrito', 'public.' || tablename, 'DELETE') AS pode_excluir
FROM pg_tables WHERE schemaname='public'
AND tablename IN ('customers','accounts','cards','merchants','transactions')
ORDER BY tablename;

SELECT rolname, rolcanlogin, rolreplication, rolsuper, rolcreatedb, rolcreaterole, rolbypassrls
FROM pg_roles WHERE rolname='cdc_restrito';

SELECT parent.rolname AS grupo
FROM pg_auth_members m
JOIN pg_roles parent ON parent.oid=m.roleid
JOIN pg_roles member ON member.oid=m.member
WHERE member.rolname='cdc_restrito';
