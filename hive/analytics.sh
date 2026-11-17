#!/usr/bin/env bash
# ============================================================
# Phase 5 - Execute les 8 requetes analytiques et sauvegarde le resultat
# dans docs/hive_resultats.txt (a reutiliser dans le rapport).
# Usage (depuis la racine du projet) : bash hive/analytics.sh
# ============================================================
cd "$(dirname "$0")/.." || exit 1
mkdir -p logs docs
source hive/lib.sh
wait_hs2 || exit 1

echo "==> Requetes analytiques (3 a 6 minutes)"
runfile 04_analytics.hql --outputformat=table 2>&1 \
  | grep -vE 'SLF4J|Failed to create directory|^No such file or directory$' \
  | cat -s | tee docs/hive_resultats.txt
if grep -q "^Error:" docs/hive_resultats.txt; then
  echo; echo "Resultat : [KO] une requete a echoue (voir ci-dessus)"; exit 1
fi
echo; echo "Resultat : [OK] 8 requetes executees. Sauvegarde : docs/hive_resultats.txt"
