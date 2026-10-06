# Pipeline de pagamentos em tempo real

**Autor: Vando Ramos**

Desafio educacional IBM/DIO com Neon PostgreSQL, Confluent Cloud, Kafka e Flink.

O projeto captura alterações via CDC, utiliza contratos Avro, enriquece pagamentos com informações de contas e detecta três transações positivas do mesmo cartão em até 60 segundos. Os alertas são consumidos em Python e persistidos no SQLite.

## Resultado validado

O teste integrado gerou um alerta de três transações, total de 60.00 em 20 segundos, recebido pelo consumidor e salvo uma vez para a chave de negócio testada.

## Documentação

Acesse o [README completo do desafio](pipeline-pagamentos/desafio-pagamentos/README.md) para consultar arquitetura, execução, testes, segurança e limitações.

O código está em [pipeline-pagamentos/desafio-pagamentos](pipeline-pagamentos/desafio-pagamentos).

## Escopo

Projeto de laboratório com dados sintéticos. A automação integral, métricas e encerramento completo dos recursos permanecem documentados como pendências.
