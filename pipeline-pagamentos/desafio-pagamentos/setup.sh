#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
[[ -f .env ]] || { echo 'Copie .env.example para .env e preencha localmente.'; exit 1; }
set -a; source .env; set +a
for c in psql python3; do command -v "$c" >/dev/null || { echo "Instale $c"; exit 1; }; done
: "${NEON_ADMIN_URL:?}" "${CDC_PASSWORD:?}"
psql "$NEON_ADMIN_URL" -X -v ON_ERROR_STOP=1 -f sql/01-foundation.sql
psql "$NEON_ADMIN_URL" -X -v ON_ERROR_STOP=1 -v cdc_password="$CDC_PASSWORD" -f sql/02-cdc-user.sql
# Não duplica seed; falha se banco parcialmente preenchido.
n=$(psql "$NEON_ADMIN_URL" -X -Atc 'SELECT count(*) FROM customers')
if [[ "$n" = 0 ]]; then psql "$NEON_ADMIN_URL" -X -v ON_ERROR_STOP=1 -f sql/03-seed.sql; fi
psql "$NEON_ADMIN_URL" -X -v ON_ERROR_STOP=1 -f sql/04-counts.sql
python3 scripts/render_connector.py
echo 'Banco e config preparados. Complete provisionamento/Flink conforme README.'
echo 'Este setup ainda NÃO provisiona todo o pipeline cloud automaticamente.'
