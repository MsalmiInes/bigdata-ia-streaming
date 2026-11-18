#!/usr/bin/env bash
# ============================================================
# Phase 8 - CSV / ORC / Parquet pour UN jeu de donnees : taille sur disque + temps de requete Hive.
# Usage (depuis la racine du projet) : bash bench/formats.sh <small|medium|large> [repetitions]
# Resultats ajoutes a bench/results/formats.csv  (run,size,metric,variant,value)
# ============================================================
cd "$(dirname "$0")/.." || exit 1
source bench/lib.sh
SIZE="${1:?usage : bash bench/formats.sh <small|medium|large> [repetitions]}"
REPS="${2:-3}"
DB="bench_${SIZE}"
BASE="/bench/${SIZE}"
FORMATS_CSV="$RES/formats.csv"
[ -f "$FORMATS_CSV" ] || echo "run,size,metric,variant,value" > "$FORMATS_CSV"
fmt() { echo "$RUN_ID,$SIZE,$1,$2,$3" >> "$FORMATS_CSV"; }
bytes() { $T bash -c "hdfs dfs -du -s '$1' 2>/dev/null | awk '{print \$1}'" | tr -d '\r'; }
mb()    { awk -v b="$1" 'BEGIN{printf "%.2f", b/1048576}'; }

echo "== TAILLE SUR DISQUE (HDFS, une copie) : jeu '$SIZE' =="
for t in $TABLES_ALL; do
  case "$t" in kafka_events) o=events ;; *) o=$t ;; esac
  c=$(bytes "$BASE/clean/$t"); r=$(bytes "$BASE/warehouse/orc/$o")
  [ -n "$c" ] && fmt size_mb "$t:csv" "$(mb "$c")"
  [ -n "$r" ] && fmt size_mb "$t:orc" "$(mb "$r")"
  printf "  %-14s CSV %9s Mo | ORC %9s Mo\n" "$t" "$(mb "${c:-0}")" "$(mb "${r:-0}")"
done
p=$(bytes "$BASE/warehouse/parquet/ratings")
[ -n "$p" ] && fmt size_mb "ratings:parquet" "$(mb "$p")" && printf "  %-14s Parquet %6s Mo\n" ratings "$(mb "$p")"

run_sql() { $HS2 beeline -u "${JDBC}${DB}" --silent=true -e "SET hive.compute.query.using.stats=false; $1" >/dev/null 2>&1; }
avg_time() {   # avg_time "<sql>" -> secondes moyennes (connexion beeline incluse), vide si echec
  local tot=0 i s e
  for i in $(seq 1 "$REPS"); do
    s=$(now); run_sql "$1" || return 1; e=$(now)
    tot=$(awk -v t="$tot" -v s="$s" -v e="$e" 'BEGIN{print t+e-s}')
  done
  awk -v t="$tot" -v n="$REPS" 'BEGIN{printf "%.1f", t/n}'
}
measure() {    # measure <metrique> <variante> "<sql>"
  local v; v=$(avg_time "$3")
  if [ -n "$v" ]; then fmt "$1" "$2" "$v"; printf "  %-12s %-22s %8s s\n" "$1" "$2" "$v"
  else printf "  %-12s %-22s %8s\n" "$1" "$2" "ECHEC"; fi
}

echo
echo "== TEMPS DE REQUETE (moyenne de $REPS executions, connexion beeline incluse) =="
measure baseline connexion "SELECT 1;"
for v in csv:ratings_csv orc:ratings_orc parquet:ratings_pq; do
  name="${v%%:*}"; tbl="${v##*:}"
  measure q_filter "$name" "SELECT ROUND(AVG(rating),4) FROM $tbl WHERE rating >= 4.0;"
  measure q_group  "$name" "SELECT COUNT(*), ROUND(AVG(c),2) FROM (SELECT movie_id, COUNT(*) AS c FROM $tbl GROUP BY movie_id) x;"
done
measure prune partition "SELECT COUNT(*), ROUND(AVG(rating),4) FROM ratings_orc WHERE rating_year = 2025;"
measure prune sans_partition "SELECT COUNT(*), ROUND(AVG(rating),4) FROM ratings_orc WHERE YEAR(ts) = 2025;"
echo
echo "Resultats ajoutes a bench/results/formats.csv"
