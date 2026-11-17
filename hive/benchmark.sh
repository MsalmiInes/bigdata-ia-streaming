#!/usr/bin/env bash
# ============================================================
# Phase 5 - Comparaison CSV / ORC / Parquet : taille sur disque et temps de requete.
# Resultats sauvegardes dans docs/hive_benchmark.txt (a reutiliser dans le rapport).
# Usage (depuis la racine du projet) : bash hive/benchmark.sh      (3 a 8 minutes)
# Le temps mesure = duree de beeline moins la duree d'un "SELECT 1" (connexion).
# Chaque requete est lancee 3 fois : moyenne affichee.
# ============================================================
cd "$(dirname "$0")/.." || exit 1
mkdir -p logs docs
source hive/lib.sh
wait_hs2 || exit 1
OUT=docs/hive_benchmark.txt

now() { date +%s.%N; }
run_sql() { $HS2 beeline -u "${JDBC}streaming" --silent=true -e "SET hive.compute.query.using.stats=false; $1" >/dev/null 2>&1; }

avg_time() {   # avg_time "sql" -> secondes (moyenne de 3, hors connexion)
  local tot=0 s e i
  for i in 1 2 3; do
    s=$(now); run_sql "$1"; e=$(now)
    tot=$(awk -v t="$tot" -v s="$s" -v e="$e" 'BEGIN{print t+e-s}')
  done
  awk -v t="$tot" -v b="$BASE" 'BEGIN{r=t/3-b; if (r<0) r=0; printf "%.1f", r}'
}

mb() { awk -v b="$1" 'BEGIN{printf "%.2f", b/1048576}'; }

{
echo "=== BENCHMARK HIVE : CSV / ORC / Parquet  ($(date '+%Y-%m-%d %H:%M')) ==="
echo
echo "--- 1) TAILLE SUR DISQUE (HDFS, une copie) ---"
printf "%-15s %12s %12s %12s %10s\n" TABLE "CSV (Mo)" "ORC (Mo)" "Parquet(Mo)" "CSV/ORC"
for t in ratings movies users subscriptions tags watch_logs events; do
  case "$t" in events) d=kafka_events ;; *) d=$t ;; esac
  C=$(hdfs_bytes "/streaming/clean/$d"); O=$(hdfs_bytes "/streaming/warehouse/orc/$t")
  if [ "$t" = "ratings" ]; then P=$(mb "$(hdfs_bytes /streaming/warehouse/parquet/ratings)"); else P="-"; fi
  R=$(awk -v c="$C" -v o="$O" 'BEGIN{ if (o>0) printf "x%.1f", c/o; else print "?" }')
  printf "%-15s %12s %12s %12s %10s\n" "$t" "$(mb "$C")" "$(mb "$O")" "$P" "$R"
done
} | tee "$OUT"

echo | tee -a "$OUT"
echo "--- 2) TEMPS DE REQUETE sur ratings (moyenne de 3 executions, en secondes) ---" | tee -a "$OUT"
echo "(mesure en cours : environ 25 lancements de beeline)"
BASE=0; BASE=$(avg_time "SELECT 1;")
echo "  duree de connexion beeline retiree : ${BASE}s" | tee -a "$OUT"

QA="SELECT ROUND(AVG(rating),4) FROM %s WHERE rating >= 4.0;"
QB="SELECT COUNT(*), ROUND(AVG(c),2) FROM (SELECT movie_id, COUNT(*) AS c FROM %s GROUP BY movie_id) x;"

{
printf "%-44s %12s %12s\n" REQUETE "A: filtre" "B: groupe"
for t in ratings_csv ratings_orc ratings_pq; do
  a=$(avg_time "$(printf "$QA" $t)")
  b=$(avg_time "$(printf "$QB" $t)")
  printf "%-44s %12s %12s\n" "$t" "$a" "$b"
done
echo
echo "--- 3) ELAGAGE DE PARTITIONS (ratings_orc, partitionnee par annee) ---"
p1=$(avg_time "SELECT COUNT(*), ROUND(AVG(rating),4) FROM ratings_orc WHERE rating_year = 2025;")
p2=$(avg_time "SELECT COUNT(*), ROUND(AVG(rating),4) FROM ratings_orc WHERE YEAR(ts) = 2025;")
printf "%-60s %8ss\n" "filtre sur la colonne de partition (rating_year = 2025)" "$p1"
printf "%-60s %8ss\n" "meme resultat sans utiliser la partition (YEAR(ts) = 2025)" "$p2"
echo
echo "A : AVG(rating) avec filtre rating >= 4   B : GROUP BY movie_id puis agregation"
echo "Sur un petit jeu de donnees, le temps est domine par le demarrage du job :"
echo "l'ecart entre formats devient net avec le jeu medium puis large (phase 8)."
} | tee -a "$OUT"

echo
echo "Sauvegarde : $OUT"
