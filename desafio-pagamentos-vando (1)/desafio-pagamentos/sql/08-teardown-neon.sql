-- Somente depois de excluir conector e verificar slot inativo.
SELECT slot_name,active FROM pg_replication_slots WHERE slot_name='desafio_slot';
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='desafio_slot' AND NOT active;
DROP PUBLICATION IF EXISTS desafio_pub;
-- Mantém os dados para consulta. Exclua branch/projeto Neon exclusivo no console.
