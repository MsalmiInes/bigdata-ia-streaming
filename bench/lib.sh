# ============================================================
# Fonctions communes du benchmark (a charger avec : source bench/lib.sh).
# A utiliser depuis la racine du projet.
# ============================================================
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RES="$ROOT/bench/results"
TIMINGS="$RES/timings.csv"
SIZES_CSV="$RES/sizes.csv"
COUNTS_CSV="$RES/counts.csv"
mkdir -p "$RES" "$ROOT/bench/tmp" "$ROOT/logs" "$ROOT/spark/results" "$ROOT/docs"
[ -f "$TIMINGS" ]    || echo "run,size,stage,seconds,status,note" > "$TIMINGS"
[ -f "$SIZES_CSV" ]  || echo "run,size,total_rows,ratings_rows,csv_mb" > "$SIZES_CSV"
[ -f "$COUNTS_CSV" ] || echo "run,size,table,pig_clean,expected" > "$COUNTS_CSV"
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"; export RUN_ID

T="docker compose exec -T tools"
HS2="docker compose exec -T hiveserver2"
JDBC="jdbc:hive2://localhost:10000/"
TABLES_ALL="ratings movies users subscriptions tags watch_logs kafka_events"

now()     { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.1f", b-a}'; }
hcount()  { $T bash -c "hdfs dfs -cat '$1' 2>/dev/null | wc -l" | tr -d '\r'; }

record() {   # record taille etape secondes statut note
  local note="${5//,/;}"
  echo "$RUN_ID,$1,$2,$3,$4,$note" >> "$TIMINGS"
}

# stage <taille> <etape> <commande ou fonction...> : chronometre, enregistre, journalise dans logs/
stage() {
  local size="$1" name="$2"; shift 2
  local log="$ROOT/logs/bench_${size}_${name}.log" t0 t1 s rc
  printf "  %-36s " "$name ($size)"
  t0=$(now)
  "$@" > "$log" 2>&1
  rc=$?
  t1=$(now); s=$(elapsed "$t0" "$t1")
  if [ "$rc" -eq 0 ]; then
    record "$size" "$name" "$s" ok ""
    echo "[OK] ${s} s"
  else
    record "$size" "$name" "$s" fail "code $rc"
    echo "[KO] apres ${s} s  (journal : logs/bench_${size}_${name}.log)"
    grep -hiE "^Error:|ERROR|Exception|FAILED|\[KO\]" "$log" | grep -v SLF4J | head -4 | sed 's/^/        /'
  fi
  return "$rc"
}

# ---------------------------------------------------------------- services
wait_core() {      # HDFS repond et YARN a un NodeManager
  local i
  for i in $(seq 1 60); do
    if $T hdfs dfs -ls / >/dev/null 2>&1 && $T yarn node -list 2>/dev/null | grep -q RUNNING; then return 0; fi
    sleep 5
  done
  return 1
}
wait_hs2() {
  local i
  for i in $(seq 1 40); do
    $HS2 beeline -u "$JDBC" --silent=true -e "SHOW DATABASES;" >/dev/null 2>&1 && return 0
    sleep 8
  done
  return 1
}
workers_alive() {
  $T curl -s http://spark-master:8080/json/ 2>/dev/null \
    | grep -Eo '"aliveworkers"[[:space:]]*:[[:space:]]*[0-9]+' | grep -Eo '[0-9]+$'
}
wait_workers() {   # wait_workers N
  local i
  for i in $(seq 1 45); do
    [ "$(workers_alive)" = "$1" ] && return 0
    sleep 4
  done
  return 1
}

# ---------------------------------------------------------------- jeux de donnees
record_size() {    # record_size <taille> : volumes -> sizes.csv
  local size="$1" d="$ROOT/data/generated/$1" total=0 rr=0 n t kb
  for t in $TABLES_ALL; do
    n=$(( $(wc -l < "$d/$t.csv") - 1 )); total=$((total + n))
    [ "$t" = "ratings" ] && rr=$n
  done
  kb=$(du -ck "$d"/*.csv | tail -1 | cut -f1)
  echo "$RUN_ID,$size,$total,$rr,$(awk -v k="$kb" 'BEGIN{printf "%.1f", k/1024}')" >> "$SIZES_CSV"
}

# Resultat attendu du nettoyage Pig, recalcule independamment avec awk : "<lignes propres> <rejets>"
expected_clean() {   # expected_clean <table> <fichier.csv>
  local f="$2"
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

# Copie les scripts Pig / HQL dans bench/tmp/<taille>/ en remplacant /streaming/ par /bench/<taille>/
# et la base Hive "streaming" par "bench_<taille>". Refuse de continuer s'il reste une reference a /streaming.
prepare_scripts() {   # prepare_scripts <taille>
  local size="$1" out="$ROOT/bench/tmp/$1" f
  rm -rf "$out"; mkdir -p "$out"
  for f in "$ROOT"/pig/0[1-4]*.pig; do
    sed -e "s#/streaming/#/bench/${size}/#g" -e "s#job.name 'pig-#job.name 'bench-${size}-pig-#" \
        "$f" > "$out/$(basename "$f")"
  done
  for f in "$ROOT"/hive/0[1-3]*.hql; do
    sed -e "s#/streaming/#/bench/${size}/#g" \
        -e "s#EXISTS streaming;#EXISTS bench_${size};#" -e "s#^USE streaming;#USE bench_${size};#" \
        "$f" > "$out/$(basename "$f")"
  done
  if grep -nE "/streaming|streaming;" "$out"/*.pig "$out"/*.hql | grep -v '^\S*:[0-9]*:--'; then
    echo "[KO] reference a /streaming restante dans les scripts prepares (voir ci-dessus)"; return 1
  fi
  return 0
}
