\set ON_ERROR_STOP on
-- Executar como owner somente depois de excluir o conector.
-- Mantem as tabelas e os dados do laboratorio.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_replication_slots
    WHERE slot_name = 'desafio_slot' AND active
  ) THEN
    RAISE EXCEPTION 'desafio_slot ativo: interrompa o conector antes do teardown.';
  END IF;
END $$;

SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name = 'desafio_slot' AND NOT active;

DROP PUBLICATION IF EXISTS desafio_pub;
-- Excluir branch/projeto Neon exclusivo pelo console, quando autorizado.
