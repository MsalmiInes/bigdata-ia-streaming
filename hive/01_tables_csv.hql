-- ============================================================
-- 01_tables_csv.hql : base "streaming" + tables EXTERNES sur les donnees propres de Pig
-- Une table externe ne copie rien : elle decrit des fichiers CSV deja presents dans HDFS
-- (supprimer la table ne supprime pas les fichiers).
-- Les dossiers de Pig contiennent des fichiers part-* et _SUCCESS : Hive ignore les fichiers
-- dont le nom commence par un underscore.
-- Une valeur vide dans une colonne numerique est lue comme NULL.
-- ============================================================
CREATE DATABASE IF NOT EXISTS streaming;
USE streaming;

DROP TABLE IF EXISTS ratings_csv;
CREATE EXTERNAL TABLE ratings_csv (user_id INT, movie_id INT, rating DOUBLE, ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/streaming/clean/ratings';

DROP TABLE IF EXISTS movies_csv;
CREATE EXTERNAL TABLE movies_csv (movie_id INT, title STRING, genres STRING, `year` INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/streaming/clean/movies';

DROP TABLE IF EXISTS users_csv;
CREATE EXTERNAL TABLE users_csv (user_id INT, name STRING, age INT, gender STRING, country STRING, signup_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/streaming/clean/users';

DROP TABLE IF EXISTS subscriptions_csv;
CREATE EXTERNAL TABLE subscriptions_csv (subscription_id INT, user_id INT, plan STRING, start_date STRING,
  end_date STRING, status STRING, monthly_price DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/streaming/clean/subscriptions';

DROP TABLE IF EXISTS tags_csv;
CREATE EXTERNAL TABLE tags_csv (user_id INT, movie_id INT, tag STRING, ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/streaming/clean/tags';

DROP TABLE IF EXISTS watch_logs_csv;
CREATE EXTERNAL TABLE watch_logs_csv (log_id BIGINT, user_id INT, movie_id INT, watch_duration_min INT,
  device STRING, ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/streaming/clean/watch_logs';

DROP TABLE IF EXISTS events_csv;
CREATE EXTERNAL TABLE events_csv (user_id INT, movie_id INT, event_type STRING, ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/streaming/clean/kafka_events';

SHOW TABLES;
