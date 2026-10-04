-- Execute somente depois do snapshot e com captura Kafka já aberta.
INSERT INTO merchants VALUES (900001,'Demo CDC','retail');
UPDATE merchants SET name='Demo CDC atualizado' WHERE id=900001;
DELETE FROM merchants WHERE id=900001;
-- Preserva as contagens finais; observe c,u,d e tombstone de mesma chave.
