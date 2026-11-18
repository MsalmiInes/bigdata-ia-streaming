#!/usr/bin/env bash
# ============================================================
# Phase 8 - BENCHMARK : mesure le temps de chaque etape selon le volume de donnees.
#
# Usage (depuis la racine du projet) :
#   bash bench/run.sh                                   # small + medium, toutes les phases
#   bash bench/run.sh --sizes "small medium large"      # avec le gros jeu (5 millions de notes : voir le conseil)
#   bash bench/run.sh --phases "gen load pig"           # seulement certaines phases
#   bash bench/run.sh --sizes medium --phases spark     # reprendre une phase pour un jeu
#
# Phases (dans cet ordre) :
#   gen      genere les CSV manquants (scripts/generate_data.py) et enregistre les volumes
#   load     copie les CSV dans HDFS : /bench/<taille>/raw
#   pig      les 4 scripts Pig sur les donnees de /bench/<taille>   (Hive arrete)
#   hive     les 3 scripts Hive : tables CSV, ORC / Parquet, features
#   formats  CSV / ORC / Parquet : tailles et temps de requete
#   spark    pandas (1 machine), puis ALS avec 1 puis 2 workers Spark
#
# Les donnees du projet principal (/streaming, base Hive "streaming") ne sont JAMAIS modifiees :
# le benchmark travaille dans /bench/<taille> et dans les bases Hive bench_<taille>.
# Resultats : bench/results/*.csv, spark/results/bench_*.json, logs/bench_*.log
# ============================================================
cd "$(dirname "$0")/.." || exit 1
source bench/lib.sh

SIZES="small medium"
PHASES="gen load pig hive formats spark"
REPS_FMT=3
REPS_ALS=3
while [ $# -gt 0 ]; do
  case "$1" in
    --sizes)  SIZES="$2"; shift 2 ;;
    --phases) PHASES="$2"; shift 2 ;;
    --reps)   REPS_FMT="$2"; REPS_ALS="$2"; shift 2 ;;
    -h|--help) sed -n '2,24p' "$0"; exit 0 ;;
    *) echo "Option inconnue : $1 (voir : bash bench/run.sh --help)"; exit 2 ;;
  esac
done
for s in $SIZES; do
  case "$s" in small|medium|large) ;; *) echo "Taille inconnue : $s (small, medium ou large)"; exit 2 ;; esac
done
has() { case " $PHASES " in *" $1 "*) return 0 ;; esac; return 1; }

for f in docker-compose.yml docker-compose.bench.yml scripts/generate_data.py pig/run_all.sh hive/01_tables_csv.hql \
         spark/04_bench_als.py spark/05_pandas_baseline.py; do
  [ -f "$f" ] || { echo "[KO] fichier absent : $f (installez la phase 8 depuis ~/bigdata-ia-streaming)"; exit 1; }
done
export COMPOSE_FILE=docker-compose.yml:docker-compose.bench.yml

echo "==> BENCHMARK  run $RUN_ID | tailles : $SIZES | phases : $PHASES"
echo "    Resultats : bench/results/timings.csv (ne rien lancer d'autre sur le PC pendant la mesure)"

# ------------------------------------------------------------------ gestion des services (RAM limitee a 8 Go)
core_only() {      # coeur (MySQL, HDFS, YARN, tools) seul : Hive, Spark et Kafka arretes
  docker compose stop hiveserver2 hive-metastore postgres kafka jupyter spark-worker spark-worker-2 spark-master >/dev/null 2>&1
  docker compose up -d --no-build >/dev/null 2>&1
  wait_core || { echo "[KO] HDFS / YARN ne repondent pas : docker compose ps"; return 1; }
}
core_plus_hive() {
  docker compose stop kafka jupyter spark-worker spark-worker-2 spark-master >/dev/null 2>&1
  docker compose up -d --no-build >/dev/null 2>&1
  wait_core || { echo "[KO] HDFS / YARN ne repondent pas"; return 1; }
  docker compose up -d --no-build hiveserver2 >/dev/null 2>&1
  echo "    attente de HiveServer2 (1 a 3 minutes)..."
  wait_hs2 || { echo "[KO] HiveServer2 ne repond pas : docker compose logs --tail=40 hiveserver2"; return 1; }
}
spark_only() {     # HDFS + Spark seulement : on libere YARN, MySQL et Hive (ALS lit HDFS directement)
  docker compose stop spark-worker-2 >/dev/null 2>&1                  # repart toujours de 1 seul worker
  docker compose up -d --no-build >/dev/null 2>&1                     # coeur complet : garantit que "tools" existe
  docker compose stop hiveserver2 hive-metastore postgres kafka resourcemanager nodemanager mysql >/dev/null 2>&1
  docker compose up -d --no-build --no-deps namenode datanode tools >/dev/null 2>&1   # --no-deps : ne relance pas MySQL
  docker compose up -d --no-build spark-master spark-worker jupyter >/dev/null 2>&1
  wait_workers 1 || { echo "[KO] le worker Spark n'est pas enregistre : docker compose logs --tail=30 spark-worker"; return 1; }
}

# ------------------------------------------------------------------ phase gen
phase_gen() {
  local size="$1"
  if [ ! -f "data/generated/$size/ratings.csv" ]; then
    echo "  generation du jeu '$size' (peut prendre 1 a 3 minutes)"
    stage "$size" gen docker run --rm -v "$PWD":/work -w /work python:3.11-slim \
          python scripts/generate_data.py --size "$size" || return 1
  fi
  record_size "$size"
  tail -1 "$SIZES_CSV" | awk -F, '{printf "  volumes : %s lignes au total dont %s notes, %s Mo de CSV\n", $3, $4, $5}'
}

# ------------------------------------------------------------------ phase load
load_raw() {
  local size="$1" t
  $T hdfs dfs -rm -r -f -skipTrash "/bench/$size" || true
  for t in $TABLES_ALL; do
    $T hdfs dfs -mkdir -p "/bench/$size/raw/$t" || return 1
    $T hdfs dfs -put -f "/work/data/generated/$size/$t.csv" "/bench/$size/raw/$t/$t.csv" || return 1
  done
}
verify_load() {
  local size="$1" t L H ok=0
  for t in $TABLES_ALL; do
    L=$(wc -l < "data/generated/$size/$t.csv"); H=$(hcount "/bench/$size/raw/$t/$t.csv")
    [ "$L" = "$H" ] || { echo "  [KO] $t : local $L, HDFS ${H:-?}"; ok=1; }
  done
  return $ok
}
phase_load() {
  local size="$1"
  stage "$size" load load_raw "$size" || return 1
  if verify_load "$size"; then echo "  verification              [OK] 7 fichiers identiques (lignes locales = lignes HDFS)"
  else echo "  verification              [KO]"; return 1; fi
}

# ------------------------------------------------------------------ phase pig
pig_script() { $T pig -x mapreduce -M -l /work/logs/ -f "/work/bench/tmp/$1/$2.pig"; }
verify_pig() {
  local size="$1" t E P ok=0
  for t in $TABLES_ALL; do
    read -r E _ <<< "$(expected_clean "$t" "data/generated/$size/$t.csv")"
    P=$(hcount "/bench/$size/clean/$t/part-*")
    echo "$RUN_ID,$size,$t,${P:-},$E" >> "$COUNTS_CSV"
    if [ "$P" = "$E" ]; then echo "[OK] $t : Pig $P = attendu $E"; else echo "[KO] $t : Pig ${P:-?} / attendu $E"; ok=1; fi
  done
  return $ok
}
phase_pig() {
  local size="$1" s
  $T hdfs dfs -test -e "/bench/$size/raw/ratings/ratings.csv" 2>/dev/null \
    || { echo "  [KO] donnees absentes de HDFS : lancez d'abord la phase load pour '$size'"; return 1; }
  prepare_scripts "$size" || return 1
  $T hdfs dfs -rm -r -f -skipTrash "/bench/$size/clean" "/bench/$size/rejects" "/bench/$size/features" >/dev/null 2>&1
  for s in 01_clean_ratings 02_clean_reference 03_clean_logs_events 04_transform; do
    stage "$size" "pig_$s" pig_script "$size" "$s" || return 1
  done
  stage "$size" check_pig verify_pig "$size" && echo "  controle des resultats    [OK] 7 tables = calcul independant (awk)" \
                                              || echo "  controle des resultats    [KO] voir logs/bench_${size}_check_pig.log"
}

# ------------------------------------------------------------------ phase hive
hive_script() {
  local size="$1" f="$2" out rc
  docker compose cp "bench/tmp/$size/$f.hql" "hiveserver2:/tmp/bench_${size}_$f.hql" || return 1
  out=$($HS2 beeline -u "$JDBC" --silent=true -f "/tmp/bench_${size}_$f.hql" 2>&1); rc=$?
  echo "$out" | grep -v SLF4J
  echo "$out" | grep -q "^Error:" && return 1
  return $rc
}
verify_hive() {
  local size="$1" out R U M ok=0
  out=$($HS2 beeline -u "${JDBC}bench_${size}" --silent=true --showHeader=false --outputformat=csv2 -e "
SET hive.compute.query.using.stats=false;
SELECT 'ratings_orc', COUNT(*) FROM ratings_orc;
SELECT 'ml_ratings', COUNT(*) FROM ml_ratings;
SELECT 'ml_user_features', COUNT(*) FROM ml_user_features;
SELECT 'ml_movie_features', COUNT(*) FROM ml_movie_features;" 2>/dev/null)
  echo "$out"
  R=$(hcount "/bench/$size/clean/ratings/part-*"); U=$(hcount "/bench/$size/clean/users/part-*"); M=$(hcount "/bench/$size/clean/movies/part-*")
  chk() { [ -n "$2" ] && [ "$(echo "$out" | awk -F, -v k="$1" '$1==k{print $2}')" = "$2" ] || { echo "[KO] $1 : attendu $2"; ok=1; }; }
  chk ratings_orc "$R"; chk ml_ratings "$R"; chk ml_user_features "$U"; chk ml_movie_features "$M"
  return $ok
}
phase_hive() {
  local size="$1" s
  $T hdfs dfs -test -e "/bench/$size/clean/ratings/_SUCCESS" 2>/dev/null \
    || { echo "  [KO] donnees nettoyees absentes : lancez d'abord la phase pig pour '$size'"; return 1; }
  prepare_scripts "$size" || return 1
  $T hdfs dfs -rm -r -f -skipTrash "/bench/$size/warehouse" >/dev/null 2>&1
  for s in 01_tables_csv 02_tables_orc_parquet 03_features_ml; do
    stage "$size" "hive_$s" hive_script "$size" "$s" || return 1
  done
  stage "$size" check_hive verify_hive "$size" && echo "  controle des resultats    [OK] Hive = Pig (notes, utilisateurs, films)" \
                                               || echo "  controle des resultats    [KO] voir logs/bench_${size}_check_hive.log"
}

# ------------------------------------------------------------------ phase spark
pandas_run() {
  docker compose exec -T -e DATA_ROOT=/data/generated -e RESULTS_DIR=/workspace/results jupyter \
    python3 -u /workspace/05_pandas_baseline.py --size "$1"
}
als_run() {   # als_run <taille> <workers>
  local size="$1" w="$2" extra=""
  [ "$size" = "large" ] && extra="--no-reco"
  docker compose exec -T \
    -e "ML_BASE=hdfs://namenode:9000/bench/$size/warehouse/ml" -e "HDFS_OUT=hdfs://namenode:9000/bench/$size" \
    -e RESULTS_DIR=/workspace/results jupyter \
    python3 -u /workspace/04_bench_als.py --size "$size" --workers "$w" --reps "$REPS_ALS" $extra
}
phase_spark() {
  local size w ready=""
  for size in $SIZES; do
    if $T hdfs dfs -test -e "/bench/$size/warehouse/ml/ratings" 2>/dev/null; then ready="$ready $size"
    else echo "  [!] jeu '$size' : tables de features absentes (phase hive non faite) : ALS ignore, pandas seulement"; fi
  done
  echo "  -- Reference une seule machine (pandas) --"
  for size in $SIZES; do stage "$size" pandas pandas_run "$size"; done
  for w in 1 2; do
    echo "  -- ALS avec $w worker(s) Spark --"
    if [ "$w" = "2" ]; then
      docker compose up -d --no-build spark-worker-2 >/dev/null 2>&1
      wait_workers 2 || { echo "[KO] le 2e worker n'est pas enregistre : docker compose logs --tail=30 spark-worker-2"; return 1; }
    fi
    for size in $ready; do stage "$size" "als_${w}w" als_run "$size" "$w"; done
  done
  docker compose stop spark-worker-2 >/dev/null 2>&1
}

# ------------------------------------------------------------------ deroulement
STARTED=$(now)
if has gen; then
  for size in $SIZES; do
    echo; echo "### $size : generation des donnees"
    phase_gen "$size" || { echo "  -> jeu '$size' ignore pour la suite"; SIZES=$(echo " $SIZES " | sed "s/ $size / /" | xargs); }
  done
fi
if has load; then
  echo; echo "################ CHARGEMENT DANS HDFS"; core_only || exit 1
  for size in $SIZES; do echo; echo "### $size : chargement"; phase_load "$size"; done
fi
if has pig; then
  echo; echo "################ PIG (Hive arrete)"; core_only || exit 1
  for size in $SIZES; do echo; echo "### $size : Pig"; phase_pig "$size"; done
fi
if has hive || has formats; then
  echo; echo "################ HIVE"; core_plus_hive || exit 1
  for size in $SIZES; do
    echo; echo "### $size : Hive"
    has hive && phase_hive "$size"
    if has formats; then
      $T hdfs dfs -test -e "/bench/$size/warehouse/orc/ratings" 2>/dev/null \
        && stage "$size" formats bash bench/formats.sh "$size" "$REPS_FMT" \
        || echo "  [!] formats ignores : tables ORC absentes pour '$size' (phase hive d'abord)"
    fi
  done
  docker compose stop hiveserver2 hive-metastore postgres >/dev/null 2>&1
fi
if has spark; then
  echo; echo "################ SPARK (YARN, MySQL et Hive arretes)"
  spark_only && phase_spark
  echo; echo "==> Retour a l'etat normal (coeur demarre)"
  docker compose stop jupyter spark-worker spark-worker-2 spark-master >/dev/null 2>&1
  docker compose up -d --no-build >/dev/null 2>&1
fi

echo
echo "==> Termine en $(elapsed "$STARTED" "$(now)") s. Synthese : bash bench/report.sh"
