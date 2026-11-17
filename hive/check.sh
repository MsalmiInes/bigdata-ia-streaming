#!/usr/bin/env bash
# ============================================================
# Phase 5 - Verification : nombre de lignes identique entre
#   Pig (HDFS) = table CSV Hive = table ORC (= Parquet pour ratings),
# puis controle des tables de features.
# Usage (depuis la racine du projet) : bash hive/check.sh
# ============================================================
cd "$(dirname "$0")/.." || exit 1
mkdir -p logs
source hive/lib.sh
wait_hs2 || exit 1
OK=0; KO=0

# table Hive -> dossier Pig
declare -A PIGDIR=( [ratings]=ratings [movies]=movies [users]=users [subscriptions]=subscriptions
                    [tags]=tags [watch_logs]=watch_logs [events]=kafka_events )
TABLES="ratings movies users subscriptions tags watch_logs events"

# Une requete COUNT par table (statements separes, une seule session beeline)
Q="SET hive.compute.query.using.stats=false;"
add() { Q="$Q SELECT '$1', COUNT(*) FROM $1;"; }
for t in $TABLES; do add "${t}_csv"; add "${t}_orc"; done
add ratings_pq; add ml_ratings; add ml_user_features; add ml_movie_features

echo "==> Comptage de toutes les tables (3 a 8 minutes : 18 requetes)"
$HS2 beeline -u "${JDBC}streaming" --silent=true --showHeader=false --outputformat=csv2 \
   -e "$Q" > logs/hive_counts.csv 2> logs/hive_counts.err
if ! grep -q "^ratings_csv," logs/hive_counts.csv; then
  echo "[KO] le comptage a echoue. Messages d'erreur utiles :"
  grep -hiE "error|failed|exception|caused by" logs/hive_counts.err logs/hive_counts.csv | head -10
  echo "(fichiers complets : logs/hive_counts.err et logs/hive_counts.csv)"
  exit 1
fi
cnt() { awk -F, -v k="$1" '$1==k{print $2}' logs/hive_counts.csv; }
MISSING=$(for t in $TABLES; do echo "${t}_csv ${t}_orc"; done | tr ' ' '\n' | while read -r k; do [ -z "$(cnt "$k")" ] && echo -n "$k "; done)
[ -n "$MISSING" ] && echo "Attention : comptage absent pour : $MISSING (voir logs/hive_counts.err)"

echo
printf "%-15s %10s %10s %10s   %s\n" TABLE "PIG(HDFS)" "HIVE CSV" "HIVE ORC" VERIFICATION
for t in $TABLES; do
  P=$(hdfs_lines "/streaming/clean/${PIGDIR[$t]}/part-*"); C=$(cnt "${t}_csv"); O=$(cnt "${t}_orc")
  if [ -n "$P" ] && [ "$P" = "$C" ] && [ "$C" = "$O" ]; then S="[OK]"; OK=$((OK+1)); else S="[KO]"; KO=$((KO+1)); fi
  printf "%-15s %10s %10s %10s   %s\n" "$t" "$P" "${C:-?}" "${O:-?}" "$S"
done

echo
chk() {   # chk libelle attendu obtenu
  if [ -n "$3" ] && [ "$2" = "$3" ]; then printf "  [OK] %-42s %s = %s\n" "$1" "$2" "$3"; OK=$((OK+1));
  else printf "  [KO] %-42s attendu %s, obtenu %s\n" "$1" "$2" "${3:-?}"; KO=$((KO+1)); fi
}
chk "ratings Parquet = ratings CSV"        "$(cnt ratings_csv)" "$(cnt ratings_pq)"
chk "ml_ratings = ratings"                 "$(cnt ratings_orc)" "$(cnt ml_ratings)"
chk "ml_user_features = utilisateurs"      "$(cnt users_orc)"   "$(cnt ml_user_features)"
chk "ml_movie_features = films"            "$(cnt movies_orc)"  "$(cnt ml_movie_features)"

echo
echo "==> Controle des features : aucun NULL, taux de churn"
$HS2 beeline -u "${JDBC}streaming" --silent=true --outputformat=csv2 -e "
SELECT SUM(CASE WHEN age IS NULL OR avg_rating IS NULL OR nb_views IS NULL THEN 1 ELSE 0 END) AS lignes_avec_null,
       SUM(age_missing) AS ages_imputes,
       ROUND(100 * AVG(churned), 1) AS taux_churn_pct
FROM ml_user_features;" 2>/dev/null | tee logs/hive_features_check.csv
NULLS=$(tail -1 logs/hive_features_check.csv | cut -d, -f1)
chk "lignes de ml_user_features avec NULL" 0 "$NULLS"

echo
echo "==> Partitions"
$HS2 beeline -u "${JDBC}streaming" --silent=true --outputformat=csv2 \
   -e "SELECT rating_year, COUNT(*) AS nb_notes FROM ratings_orc GROUP BY rating_year ORDER BY rating_year;" 2>/dev/null

echo
echo "==> Dossiers dans HDFS"
$TOOLS hdfs dfs -ls /streaming/warehouse/orc /streaming/warehouse/parquet /streaming/warehouse/ml 2>/dev/null | grep -E "^d" | awk '{print $NF}'

echo
echo "Resultat : $OK OK / $KO KO"
[ "$KO" -eq 0 ]
