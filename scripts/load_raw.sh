#!/usr/bin/env bash
# ============================================================
# Chargement des 7 CSV dans HDFS : /streaming/raw/<table>/<table>.csv
# (version simple, sans Sqoop / Flume / Kafka)
# Usage (depuis la racine du projet) : bash scripts/load_raw.sh
# ============================================================
cd "$(dirname "$0")/.." || exit 1
[ -f .env ] && { set -a; . ./.env; set +a; }
DATASET="${DATASET:-small}"
SRC="data/generated/$DATASET"
T="docker compose exec -T tools"
OK=0; KO=0

[ -d "$SRC" ] || { echo "Dossier introuvable : $SRC"; exit 1; }
echo "==> Chargement du jeu '$DATASET' dans HDFS"

for t in movies users subscriptions ratings tags watch_logs kafka_events; do
  LOCAL=$(( $(wc -l < "$SRC/$t.csv") ))           # lignes locales (en-tete incluse)
  $T hdfs dfs -mkdir -p "/streaming/raw/$t" >/dev/null 2>&1
  $T hdfs dfs -put -f "/work/$SRC/$t.csv" "/streaming/raw/$t/$t.csv" >/dev/null 2>&1
  HDFS=$($T bash -c "hdfs dfs -cat /streaming/raw/$t/$t.csv 2>/dev/null | wc -l" | tr -d '\r')
  if [ "$LOCAL" = "$HDFS" ]; then
    printf "  [OK] %-15s %9s lignes (en-tete incluse)\n" "$t" "$HDFS"; OK=$((OK+1))
  else
    printf "  [KO] %-15s local=%s hdfs=%s\n" "$t" "$LOCAL" "${HDFS:-?}"; KO=$((KO+1))
  fi
done

echo
$T hdfs dfs -du -h /streaming/raw
echo
echo "Resultat : $OK OK / $KO KO"
[ "$KO" -eq 0 ]
