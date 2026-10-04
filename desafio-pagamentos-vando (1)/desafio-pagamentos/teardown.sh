#!/usr/bin/env bash
# Escopo: somente IDs do laboratório preenchidos no .env. Não apaga cluster inteiro.
set -euo pipefail
cd "$(dirname "$0")"
set -a; source .env; set +a
: "${ENVIRONMENT_ID:?Preencha ENVIRONMENT_ID}" "${CLUSTER_ID:?Preencha CLUSTER_ID}"
confluent environment use "$ENVIRONMENT_ID"
confluent kafka cluster use "$CLUSTER_ID"
# Confira --help da sua versão: parâmetros de Flink incluem pool/cloud/region.
for id in ${FLINK_STATEMENT_IDS:-}; do
 confluent flink statement delete "$id" --force
done
if [[ -n ${CONNECTOR_ID:-} ]]; then confluent connect cluster delete "$CONNECTOR_ID" --force; fi
confluent connect cluster list
confluent flink statement list
# Espere exclusões concluírem e execute listas novamente. Não falsificar None found.
# Depois, owner deve remover SLOT inativo (sql/08-teardown-neon.sql).
# Remover manualmente pool Flink, chaves, SAs, schemas, tópicos, cluster e environment
# EXCLUSIVOS do desafio, conferindo dependências e cobranca no console.
