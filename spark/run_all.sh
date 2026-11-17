#!/usr/bin/env bash
# ============================================================
# Phase 6 - Lance les 3 scripts Spark MLlib sur le cluster Docker.
# Usage (depuis la racine du projet) :
#   bash spark/run_all.sh            # complet (validation croisee d'ALS : 10 a 25 min)
#   bash spark/run_all.sh --fast     # ALS sans reglage (rapide), churn complet
#   bash spark/run_all.sh 02         # un seul script (01, 02 ou 03), options apres : bash spark/run_all.sh 02 --fast
# ============================================================
cd "$(dirname "$0")/.." || exit 1
mkdir -p logs
ONLY=""; ARGS=()
for a in "$@"; do
  case "$a" in 01|02|03) ONLY="$a" ;; *) ARGS+=("$a") ;; esac
done
T="docker compose exec -T tools"
NOISE='^[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9:]{8} |^Setting default log level|^To adjust logging|^Using Spark|^WARNING: |NativeCodeLoader|^SLF4J|^\s+at '

echo "==> Preparation : arret de Hive et Kafka, demarrage de Spark"
docker compose stop hiveserver2 hive-metastore postgres kafka >/dev/null 2>&1
docker compose up -d --no-build >/dev/null 2>&1
docker compose up -d --no-build spark-master spark-worker jupyter >/dev/null 2>&1

echo -n "==> Attente du worker Spark "
READY=0
for i in $(seq 1 30); do
  if $T curl -s http://spark-master:8080/json/ 2>/dev/null | grep -Eq '"aliveworkers"[[:space:]]*:[[:space:]]*1'; then READY=1; break; fi
  echo -n "."; sleep 4
done
echo
[ "$READY" -eq 1 ] || { echo "[KO] le worker Spark n'est pas enregistre. Voir : docker compose logs --tail=30 spark-worker"; exit 1; }

if ! $T hdfs dfs -test -e /streaming/warehouse/ml/ratings 2>/dev/null; then
  echo "[KO] tables de features absentes de HDFS : terminez la phase 5 (bash hive/check.sh doit donner 12 OK)"; exit 1
fi

OK=0; KO=0
run_py() {   # run_py script.py [options]
  local s="$1"; shift
  [ -n "$ONLY" ] && [ "${s:0:2}" != "$ONLY" ] && return 0
  echo
  echo "################  $s $*"
  local START=$(date +%s)
  docker compose exec -T jupyter python3 -u "/workspace/$s" "$@" 2>&1 | tee "logs/spark_${s%.py}.log" | grep -vE "$NOISE"
  local RC=${PIPESTATUS[0]}
  local SEC=$(( $(date +%s) - START ))
  if [ "$RC" -eq 0 ]; then echo "    [OK] $s termine en ${SEC}s"; OK=$((OK+1))
  else echo "    [KO] $s code $RC apres ${SEC}s (journal complet : logs/spark_${s%.py}.log)"; KO=$((KO+1)); fi
}

run_py 01_check_data.py
run_py 02_als_recommender.py "${ARGS[@]}"
run_py 03_churn_prediction.py

echo
echo "Resultat : $OK script(s) OK / $KO KO"
[ "$OK" -gt 0 ] && python3 spark/report.py
[ "$KO" -eq 0 ]
