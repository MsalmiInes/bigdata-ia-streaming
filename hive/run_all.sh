#!/usr/bin/env bash
# ============================================================
# Phase 5 - Construit l'entrepot Hive : tables CSV, ORC / Parquet, features.
# Usage (depuis la racine du projet) : bash hive/run_all.sh
# Relancable : le dossier /streaming/warehouse est reconstruit a chaque fois.
# ============================================================
cd "$(dirname "$0")/.." || exit 1
mkdir -p logs docs
source hive/lib.sh

echo "==> Verification de HiveServer2"
wait_hs2 || exit 1

if ! $TOOLS hdfs dfs -test -e /streaming/clean/ratings/_SUCCESS 2>/dev/null; then
  echo "Donnees propres absentes de HDFS : terminez d'abord la phase 4 (Pig)."; exit 1
fi

echo "==> Nettoyage de /streaming/warehouse (reconstruit ensuite)"
$TOOLS hdfs dfs -rm -r -f /streaming/warehouse >/dev/null 2>&1

OK=0; KO=0
for f in hive/01_tables_csv.hql hive/02_tables_orc_parquet.hql hive/03_features_ml.hql; do
  n=$(basename "$f")
  echo "==> $n"
  START=$(date +%s)
  runfile "$n" > "logs/hive_${n%.hql}.out" 2>&1
  RC=$?
  SEC=$(( $(date +%s) - START ))
  if [ $RC -eq 0 ] && ! grep -q "^Error:" "logs/hive_${n%.hql}.out"; then
    echo "    [OK] termine en ${SEC}s"; OK=$((OK+1))
  else
    echo "    [KO] echec apres ${SEC}s. Messages d'erreur (detail : logs/hive_${n%.hql}.out) :"
    grep -iE "^Error:|FAILED|Exception" "logs/hive_${n%.hql}.out" | head -8 | sed 's/^/         /'
    KO=$((KO+1)); break
  fi
done

echo
echo "Tables creees :"
$HS2 beeline -u "${JDBC}streaming" --silent=true --outputformat=csv2 -e "SHOW TABLES;" 2>/dev/null | tail -n +2 | tr '\n' ' '
echo; echo
echo "Resultat : $OK script(s) OK / $KO KO"
[ "$KO" -eq 0 ]
