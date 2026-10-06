# Pipeline de pagamentos em tempo real

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Flink](https://img.shields.io/badge/Apache_Flink-E6526F?style=for-the-badge&logo=apacheflink&logoColor=white)
![Confluent](https://img.shields.io/badge/Confluent-FF5E1F?style=for-the-badge&logo=confluent&logoColor=white)
![Neon](https://img.shields.io/badge/Neon-00E599?style=for-the-badge&logoColor=black)
![SQLite](https://img.shields.io/badge/SQLite-003B57?style=for-the-badge&logo=sqlite&logoColor=white)


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
