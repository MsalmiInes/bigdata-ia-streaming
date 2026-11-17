-- ============================================================
-- 02_tables_orc_parquet.hql : copies en format colonne (ORC et Parquet)
--   ratings_orc     : ORC, PARTITIONNEE par annee (rating_year)
--   ratings_pq      : Parquet, meme contenu (pour comparer les formats)
--   watch_logs_orc  : ORC, PARTITIONNEE par appareil (device)
--   movies_orc, users_orc, subscriptions_orc, tags_orc, events_orc : ORC
-- Tables externes avec dossier explicite sous /streaming/warehouse :
-- Spark pourra lire ces dossiers directement (phase 6).
-- Les textes de date sont convertis en vrais types DATE et TIMESTAMP.
-- ============================================================
USE streaming;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET parquet.compression=GZIP;

-- ---------------------------------------------------------- ratings (ORC, partitionnee)
DROP TABLE IF EXISTS ratings_orc;
CREATE EXTERNAL TABLE ratings_orc (user_id INT, movie_id INT, rating DOUBLE, ts TIMESTAMP)
PARTITIONED BY (rating_year INT)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/streaming/warehouse/orc/ratings';

INSERT OVERWRITE TABLE ratings_orc PARTITION (rating_year)
SELECT user_id, movie_id, rating, CAST(ts AS TIMESTAMP), YEAR(CAST(ts AS TIMESTAMP))
FROM ratings_csv;

-- ---------------------------------------------------------- ratings (Parquet)
DROP TABLE IF EXISTS ratings_pq;
CREATE EXTERNAL TABLE ratings_pq (user_id INT, movie_id INT, rating DOUBLE, ts TIMESTAMP)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/streaming/warehouse/parquet/ratings';

INSERT OVERWRITE TABLE ratings_pq
SELECT user_id, movie_id, rating, CAST(ts AS TIMESTAMP) FROM ratings_csv;

-- ---------------------------------------------------------- watch_logs (ORC, partitionnee par appareil)
DROP TABLE IF EXISTS watch_logs_orc;
CREATE EXTERNAL TABLE watch_logs_orc (log_id BIGINT, user_id INT, movie_id INT, watch_duration_min INT, ts TIMESTAMP)
PARTITIONED BY (device STRING)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/streaming/warehouse/orc/watch_logs';

INSERT OVERWRITE TABLE watch_logs_orc PARTITION (device)
SELECT log_id, user_id, movie_id, watch_duration_min, CAST(ts AS TIMESTAMP), device
FROM watch_logs_csv;

-- ---------------------------------------------------------- movies
DROP TABLE IF EXISTS movies_orc;
CREATE EXTERNAL TABLE movies_orc (movie_id INT, title STRING, genres STRING, `year` INT)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/streaming/warehouse/orc/movies';
INSERT OVERWRITE TABLE movies_orc SELECT movie_id, title, genres, `year` FROM movies_csv;

-- ---------------------------------------------------------- users
DROP TABLE IF EXISTS users_orc;
CREATE EXTERNAL TABLE users_orc (user_id INT, name STRING, age INT, gender STRING, country STRING, signup_date DATE)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/streaming/warehouse/orc/users';
INSERT OVERWRITE TABLE users_orc
SELECT user_id, name, age, gender, country, CAST(signup_date AS DATE) FROM users_csv;

-- ---------------------------------------------------------- subscriptions
-- end_date vide (abonnement actif) devient NULL
DROP TABLE IF EXISTS subscriptions_orc;
CREATE EXTERNAL TABLE subscriptions_orc (subscription_id INT, user_id INT, plan STRING, start_date DATE,
  end_date DATE, status STRING, monthly_price DOUBLE)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/streaming/warehouse/orc/subscriptions';
INSERT OVERWRITE TABLE subscriptions_orc
SELECT subscription_id, user_id, plan, CAST(start_date AS DATE), CAST(NULLIF(end_date, '') AS DATE),
       status, monthly_price
FROM subscriptions_csv;

-- ---------------------------------------------------------- tags
DROP TABLE IF EXISTS tags_orc;
CREATE EXTERNAL TABLE tags_orc (user_id INT, movie_id INT, tag STRING, ts TIMESTAMP)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/streaming/warehouse/orc/tags';
INSERT OVERWRITE TABLE tags_orc SELECT user_id, movie_id, tag, CAST(ts AS TIMESTAMP) FROM tags_csv;

-- ---------------------------------------------------------- events
DROP TABLE IF EXISTS events_orc;
CREATE EXTERNAL TABLE events_orc (user_id INT, movie_id INT, event_type STRING, ts TIMESTAMP)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/streaming/warehouse/orc/events';
INSERT OVERWRITE TABLE events_orc SELECT user_id, movie_id, event_type, CAST(ts AS TIMESTAMP) FROM events_csv;

SHOW PARTITIONS ratings_orc;
SHOW PARTITIONS watch_logs_orc;
