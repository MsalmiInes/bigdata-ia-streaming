#!/usr/bin/env bash
# ============================================================
# Phase 4 - execute les scripts Pig sur YARN (mode MapReduce).
# Usage (depuis la racine du projet) :
#   bash pig/run_all.sh          # les 4 scripts
#   bash pig/run_all.sh 01       # un seul script (nom commencant par 01)
# -M : une instruction STORE a la fois (evite de saturer la RAM de YARN)
# ============================================================
cd "$(dirname "$0")/.." || exit 1
mkdir -p logs
T="docker compose exec -T tools"
OK=0; KO=0

if ! $T hdfs dfs -test -e /streaming/raw/ratings/ratings.csv 2>/dev/null; then
  echo "Donnees brutes absentes de HDFS. Lancez d'abord : bash scripts/load_raw.sh"; exit 1
fi

for f in pig/${1:-}*.pig; do
  s=$(basename "$f" .pig)
  echo "==> $s"
  START=$(date +%s)
  $T pig -x mapreduce -M -l /work/logs/ -f "/work/$f" > "logs/$s.out" 2>&1
  RC=$?
  SEC=$(( $(date +%s) - START ))
  if [ $RC -eq 0 ]; then
    echo "    [OK] termine en ${SEC}s"
    grep -h "Successfully stored" "logs/$s.out" \
      | sed -E 's/.*Successfully stored ([0-9]+) records.*in: "hdfs:\/\/namenode:9000(.*)"/      \1 lignes -> \2/'
    OK=$((OK+1))
  else
    echo "    [KO] echec (code $RC) apres ${SEC}s. Fin de logs/$s.out :"
    grep -E "ERROR|Exception|Failed" "logs/$s.out" | tail -8 | sed 's/^/         /'
    KO=$((KO+1))
    break
  fi
done

echo
echo "Resultat : $OK script(s) OK / $KO KO"
[ "$KO" -eq 0 ]
