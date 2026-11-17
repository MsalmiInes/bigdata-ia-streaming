#!/usr/bin/env bash
# ============================================================
# Phase 4 - Rapport qualite + verification independante.
# Pour chaque table : on recalcule avec awk (sur le CSV local) ce que Pig DOIT produire,
# puis on compare avec ce qui est reellement dans HDFS.
# Usage (depuis la racine du projet) : bash pig/report.sh
# ============================================================
cd "$(dirname "$0")/.." || exit 1
[ -f .env ] && { set -a; . ./.env; set +a; }
DATASET="${DATASET:-small}"
SRC="data/generated/$DATASET"
T="docker compose exec -T tools"
OK=0; KO=0

hcount() { $T bash -c "hdfs dfs -cat '$1' 2>/dev/null | wc -l" | tr -d '\r'; }

# Resultat attendu : "<lignes propres> <lignes rejetees>"
expected() {
  f="$SRC/$1.csv"
  case "$1" in
    ratings)
      awk -F, 'NR>1{ if ($1!="" && $2!="" && $3!="" && $3+0>=0.5 && $3+0<=5) { if(!s[$0]++) c++ } else r++ } END{print c+0, r+0}' "$f" ;;
    movies)
      awk -F, 'NR>1{ if ($1!="" && $4!="" && $4+0>=1900 && $4+0<=2030) { if(!s[$0]++) c++ } else r++ } END{print c+0, r+0}' "$f" ;;
    users)
      awk -F, 'NR>1{ if ($1!="" && ($3=="" || ($3+0>=10 && $3+0<=100))) { if(!s[$0]++) c++ } else r++ } END{print c+0, r+0}' "$f" ;;
    subscriptions)
      awk -F, 'NR>1{ if ($1!="" && $2!="" && $7!="" && $7+0>0 && ($6=="active" || $6=="cancelled")) { if(!s[$0]++) c++ } else r++ } END{print c+0, r+0}' "$f" ;;
    tags)
      awk -F, 'NR>1{ if ($1!="" && $2!="" && $3!="") { if(!s[$0]++) c++ } else r++ } END{print c+0, r+0}' "$f" ;;
    watch_logs)
      awk -F, 'NR>1{ if ($1!="" && $2!="" && $3!="" && $4!="" && $4+0>=1 && $4+0<=600 && $5 ~ /^(mobile|tv|web|tablet)$/) { if(!s[$0]++) c++ } else r++ } END{print c+0, r+0}' "$f" ;;
    kafka_events)
      awk -F, 'NR>1{ if ($1!="" && $2!="" && $4!="" && $3 ~ /^(play|pause|like|search)$/) c++; else r++ } END{print c+0, r+0}' "$f" ;;
  esac
}

echo "== NETTOYAGE PIG : jeu '$DATASET' =="
printf "%-14s %9s %9s %8s %9s %7s   %s\n" TABLE BRUT PROPRE REJETS DOUBLONS "%REJ" "VERIFICATION (awk = Pig ?)"
for t in ratings movies users subscriptions tags watch_logs kafka_events; do
  RAW=$(( $(hcount "/streaming/raw/$t/$t.csv") - 1 ))
  CLEAN=$(hcount "/streaming/clean/$t/part-*")
  REJ=$(hcount "/streaming/rejects/$t/part-*")
  DUP=$(( RAW - CLEAN - REJ ))
  read -r ECLEAN EREJ <<< "$(expected "$t")"
  PCT=$(awk -v r="$REJ" -v b="$RAW" 'BEGIN{ if (b>0) printf "%.2f", 100*r/b; else print "0.00" }')
  if [ "$CLEAN" = "$ECLEAN" ] && [ "$REJ" = "$EREJ" ]; then S="[OK]"; OK=$((OK+1)); else S="[KO] attendu propres=$ECLEAN rejets=$EREJ"; KO=$((KO+1)); fi
  printf "%-14s %9s %9s %8s %9s %7s   %s\n" "$t" "$RAW" "$CLEAN" "$REJ" "$DUP" "$PCT" "$S"
done

echo
echo "== TRANSFORMATIONS (script 04) =="
chk() {   # chk libelle attendu obtenu
  if [ -n "$3" ] && [ "$2" = "$3" ]; then printf "  [OK] %-44s %s = %s\n" "$1" "$2" "$3"; OK=$((OK+1));
  else printf "  [KO] %-44s attendu %s, obtenu %s\n" "$1" "$2" "${3:-?}"; KO=$((KO+1)); fi
}
chk "ratings_enriched = ratings propres"  "$(hcount '/streaming/clean/ratings/part-*')" "$(hcount '/streaming/features/ratings_enriched/part-*')"
chk "movie_stats = films propres"         "$(hcount '/streaming/clean/movies/part-*')"  "$(hcount '/streaming/features/movie_stats/part-*')"
chk "user_features = utilisateurs propres" "$(hcount '/streaming/clean/users/part-*')"  "$(hcount '/streaming/features/user_features/part-*')"

echo
echo "== 3 premieres lignes de chaque sortie de transformation =="
for d in ratings_enriched movie_stats user_features; do
  echo "-- $d"
  $T bash -c "hdfs dfs -cat '/streaming/features/$d/part-*' 2>/dev/null | head -3"
done

echo
echo "Resultat : $OK OK / $KO KO"
[ "$KO" -eq 0 ]
