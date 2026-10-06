#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
[[ -f .env ]] || { echo 'Copie .env.example para .env e preencha localmente.'; exit 1; }
set -a; source .env; set +a
for c in psql python3; do command -v "$c" >/dev/null || { echo "Instale $c"; exit 1; }; done
: "${NEON_ADMIN_URL:?}" "${CDC_PASSWORD:?}"
psql "$NEON_ADMIN_URL" -X -v ON_ERROR_STOP=1 -f sql/01-foundation.sql
psql "$NEON_ADMIN_URL" -X -v ON_ERROR_STOP=1 -v cdc_password="$CDC_PASSWORD" -f sql/02-cdc-user.sql
# Carrega seed somente quando todas as cinco tabelas estao vazias.
contagens=$(psql "$NEON_ADMIN_URL" -X -v ON_ERROR_STOP=1 -At -F ' ' -c '
SELECT
 (SELECT count(*) FROM public.customers),
 (SELECT count(*) FROM public.accounts),
 (SELECT count(*) FROM public.cards),
 (SELECT count(*) FROM public.merchants),
 (SELECT count(*) FROM public.transactions);')
read -r clientes contas cartoes comerciantes transacoes <<< "$contagens"
if [[ "$contagens" == "0 0 0 0 0" ]]; then
 psql "$NEON_ADMIN_URL" -X -v ON_ERROR_STOP=1 -f sql/03-seed.sql
elif (( clientes >= 200 && contas >= 240 && cartoes >= 320 &&
        comerciantes >= 60 && transacoes >= 2000 )); then
 echo 'Contagens atingem a base esperada; seed nao sera recarregado.'
else
 echo 'Contagens abaixo da base esperada. Revise o banco; seed nao executado.' >&2
 exit 1
fi
# Contagens nao comprovam integridade nem identidade dos dados existentes.
psql "$NEON_ADMIN_URL" -X -v ON_ERROR_STOP=1 -f sql/04-counts.sql
python3 scripts/render_connector.py
echo 'Banco e config preparados. Complete provisionamento/Flink conforme README.'
echo 'Este setup ainda NÃO provisiona todo o pipeline cloud automaticamente.'
