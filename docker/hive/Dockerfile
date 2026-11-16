# Hive 4.0.0 + driver PostgreSQL (metastore externe, persistant)
FROM apache/hive:4.0.0
USER root
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/hive/lib/postgres.jar
USER hive
