# Fonctions communes aux scripts Hive (a charger avec : source hive/lib.sh)
# A utiliser depuis la racine du projet.
HS2="docker compose exec -T hiveserver2"
JDBC="jdbc:hive2://localhost:10000/"
TOOLS="docker compose exec -T tools"

# Attend que HiveServer2 accepte les connexions (max 5 min)
wait_hs2() {
  if ! docker compose ps --status running --services 2>/dev/null | grep -qx hiveserver2; then
    echo "HiveServer2 n'est pas demarre. Lancez : docker compose up -d --no-build hiveserver2"
    return 1
  fi
  for i in $(seq 1 30); do
    if $HS2 beeline -u "$JDBC" --silent=true -e "SHOW DATABASES;" >/dev/null 2>&1; then return 0; fi
    echo "  attente de HiveServer2 (${i}/30)..."; sleep 10
  done
  echo "HiveServer2 ne repond pas. Voir : docker compose logs --tail=50 hiveserver2"
  return 1
}

# Copie un fichier .hql dans le conteneur puis l'execute avec beeline.
# usage : runfile 01_tables_csv.hql [options beeline...]
runfile() {
  local f="$1"; shift
  docker compose cp "hive/$f" "hiveserver2:/tmp/$f" >/dev/null 2>&1 || return 1
  $HS2 beeline -u "$JDBC" --silent=true "$@" -f "/tmp/$f"
}

# Nombre de lignes d'un dossier HDFS (fichiers part-*)
hdfs_lines() { $TOOLS bash -c "hdfs dfs -cat '$1' 2>/dev/null | wc -l" | tr -d '\r'; }
# Taille en octets d'un dossier HDFS
hdfs_bytes() { $TOOLS bash -c "hdfs dfs -du -s '$1' 2>/dev/null | awk '{print \$1}'" | tr -d '\r'; }
