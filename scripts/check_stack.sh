#!/usr/bin/env bash
# Verification de la stack par groupe.
# Usage : bash scripts/check_stack.sh core | kafka | hive | spark
cd "$(dirname "$0")/.." || exit 1
[ -f .env ] && { set -a; . ./.env; set +a; }
DATASET="${DATASET:-small}"
OK=0; KO=0

dc()  { docker compose "$@"; }
dcx() { docker compose exec -T "$@"; }

check() {   # check "libelle" fonction_de_test
  local label="$1"; shift
  local out
  if out=$("$@" 2>&1); then
    echo "  [OK] $label"; OK=$((OK+1))
  else
    echo "  [KO] $label"; echo "$out" | tail -6 | sed 's/^/         /'; KO=$((KO+1))
  fi
}

JDBC='jdbc:mysql://mysql:3306/streaming_db?useSSL=false&allowPublicKeyRetrieval=true'
EXAMPLES=/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar
K=/opt/kafka/bin

# ---------------------------------------------------------------- tests CORE
t_mysql()    { dcx mysql mysql -uroot -proot -N -e 'SELECT COUNT(*) FROM streaming_db.ratings' 2>/dev/null | grep -qx "$EXPECTED"; }
t_dn()       { dcx namenode hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes (1)'; }
t_mkdir()    { dcx tools hdfs dfs -mkdir -p /streaming/raw /streaming/clean /streaming/features /user/hive/warehouse /tmp; }
t_hdfsrw()   { echo bonjour | dcx tools hdfs dfs -put -f - /tmp/hello.txt && dcx tools hdfs dfs -cat /tmp/hello.txt | grep -q bonjour; }
t_nm()       { dcx tools yarn node -list 2>/dev/null | grep -q RUNNING; }
t_pi()       { dcx tools hadoop jar "$EXAMPLES" pi 2 10 2>&1 | grep -q 'Estimated value of Pi'; }
t_sqoopv()   { dcx tools sqoop version 2>&1 | grep -q 'Sqoop 1.4.7'; }
t_sqoopdb()  { dcx tools sqoop list-tables --connect "$JDBC" --username root --password root 2>/dev/null | grep -qx ratings; }
t_flume()    { dcx tools flume-ng version 2>&1 | grep -q 'Flume 1.11.0'; }
t_pigv()     { dcx tools pig -version 2>&1 | grep -q 'version 0.17.0'; }
t_pigjob()   {
  printf 'a\nb\na\n' | dcx tools hdfs dfs -put -f - /tmp/pig_in.txt || return 1
  dcx tools pig -x mapreduce -e "A = LOAD '/tmp/pig_in.txt' AS (w:chararray); B = GROUP A BY w; C = FOREACH B GENERATE group, COUNT(A); DUMP C;" 2>&1 | grep -q '(a,2)'
}

group_core() {
  echo "== Groupe CORE : MySQL, HDFS, YARN, outils =="
  EXPECTED=$(( $(wc -l < "data/generated/$DATASET/ratings.csv") - 1 ))
  check "MySQL : table ratings chargee ($EXPECTED lignes attendues)" t_mysql
  check "HDFS : 1 DataNode actif"                                    t_dn
  check "HDFS : arborescence /streaming creee"                       t_mkdir
  check "HDFS : ecriture + lecture d'un fichier"                     t_hdfsrw
  check "YARN : 1 NodeManager RUNNING"                               t_nm
  check "YARN : job MapReduce (calcul de Pi)"                        t_pi
  check "Sqoop : version 1.4.7"                                      t_sqoopv
  check "Sqoop : connexion MySQL (liste des tables)"                 t_sqoopdb
  check "Flume : version 1.11.0"                                     t_flume
  check "Pig : version 0.17.0"                                       t_pigv
  check "Pig : job reel sur YARN (comptage)"                         t_pigjob
}

# --------------------------------------------------------------- tests KAFKA
t_ktopic() { dcx kafka $K/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic healthcheck --partitions 1 --replication-factor 1; }
t_kmsg()   {
  echo hello-kafka | dcx kafka $K/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic healthcheck || return 1
  dcx kafka $K/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic healthcheck --from-beginning --max-messages 1 --timeout-ms 20000 2>/dev/null | grep -q hello-kafka
}
t_knet()   { dcx tools nc -z kafka 19092; }

group_kafka() {
  echo "== Groupe KAFKA =="
  check "Kafka : creation du topic healthcheck"            t_ktopic
  check "Kafka : message produit puis consomme"            t_kmsg
  check "Kafka : joignable depuis tools (kafka:19092)"     t_knet
}

# ---------------------------------------------------------------- tests HIVE
BL() { dcx hiveserver2 beeline -u jdbc:hive2://localhost:10000/ -e "$1" 2>&1; }
t_hs2()   { BL 'SHOW DATABASES;' | grep -q default; }
t_hiveq() { BL "CREATE DATABASE IF NOT EXISTS smoke; CREATE TABLE IF NOT EXISTS smoke.t (id INT, nom STRING); INSERT INTO smoke.t VALUES (1,'ines'); SELECT COUNT(*) AS n FROM smoke.t;" | grep -Eq '\| +[1-9][0-9]* +\|'; }
t_hivefs(){ dcx tools hdfs dfs -ls -R /user/hive/warehouse 2>&1 | grep -q smoke; }

group_hive() {
  echo "== Groupe HIVE =="
  check "Hive : HiveServer2 repond"                          t_hs2
  check "Hive : CREATE + INSERT + SELECT"                    t_hiveq
  check "Hive : table stockee dans HDFS (warehouse)"         t_hivefs
}

# --------------------------------------------------------------- tests SPARK
t_smaster() { dcx tools curl -sf http://spark-master:8080 >/dev/null; }
t_sworker() { dcx tools curl -s http://spark-master:8080/json/ | grep -Eq '"aliveworkers"[[:space:]]*:[[:space:]]*1'; }
t_sjob()    { dcx jupyter python3 /workspace/test_spark_hdfs.py 2>&1 | grep -q 'Lignes relues depuis HDFS : 10'; }

group_spark() {
  echo "== Groupe SPARK =="
  check "Spark : master joignable"                           t_smaster
  check "Spark : 1 worker enregistre"                        t_sworker
  check "Spark : job distribue + Parquet sur HDFS"           t_sjob
}

case "${1:-}" in
  core)  group_core ;;
  kafka) group_kafka ;;
  hive)  group_hive ;;
  spark) group_spark ;;
  *) echo "Usage : bash scripts/check_stack.sh core|kafka|hive|spark"; exit 2 ;;
esac
echo
echo "Resultat : $OK OK / $KO KO"
[ "$KO" -eq 0 ]
