# Execute par l'entrypoint MySQL au 1er demarrage (le serveur temporaire est deja actif).
# 1) copie les CSV (montes en lecture seule dans /csv) vers le dossier autorise par secure-file-priv
# 2) execute import_mysql.sql en remplacant le chemin /data/ par ce dossier
echo "[load] copie des CSV vers /var/lib/mysql-files"
cp /csv/*.csv /var/lib/mysql-files/
echo "[load] execution de import_mysql.sql"
sed 's#/data/#/var/lib/mysql-files/#g' /csv/import_mysql.sql | mysql -uroot -p"${MYSQL_ROOT_PASSWORD}"
echo "[load] termine"
