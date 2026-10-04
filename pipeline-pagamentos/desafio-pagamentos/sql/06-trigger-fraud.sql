-- Execute após normalização/enriquecimento/regra estarem RUNNING.
-- Novos IDs: se repetir, escolha outros IDs. Linhas na mesma transação <=20s.
BEGIN;
INSERT INTO transactions VALUES
(900001,1,1,1,10.00,clock_timestamp()),
(900002,1,1,1,12.00,clock_timestamp()+INTERVAL '10 seconds'),
(900003,1,1,1,15.00,clock_timestamp()+INTERVAL '20 seconds');
COMMIT;
-- Aguarde >=60s e gere evento posterior para avançar watermark.
-- INSERT INTO transactions VALUES (900004,1,1,1,1.00,clock_timestamp());
