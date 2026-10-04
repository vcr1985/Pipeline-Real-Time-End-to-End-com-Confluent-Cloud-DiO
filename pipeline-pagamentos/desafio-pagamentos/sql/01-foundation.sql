-- Execute como owner em banco exclusivo. Ative logical replication no Neon antes.
CREATE TABLE IF NOT EXISTS customers(id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS accounts(id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES customers(id), status TEXT NOT NULL, balance NUMERIC(15,2) NOT NULL);
CREATE TABLE IF NOT EXISTS cards(id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL REFERENCES accounts(id), token TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS merchants(id BIGINT PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS transactions(id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL REFERENCES accounts(id), card_id BIGINT NOT NULL REFERENCES cards(id), merchant_id BIGINT NOT NULL REFERENCES merchants(id), amount NUMERIC(15,2) NOT NULL, event_time TIMESTAMPTZ NOT NULL);
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE cards REPLICA IDENTITY FULL;
ALTER TABLE merchants REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
-- Role SQL criada abaixo pelo setup, sem privilégio de escrita nas tabelas.
