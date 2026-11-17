-- ============================================================
-- 03_features_ml.hql : tables de features pour Spark MLlib (phase 6)
--   ml_ratings        : (user_id, movie_id, rating)    -> recommandation ALS
--   ml_user_features  : une ligne par utilisateur      -> prediction du churn
--   ml_movie_features : une ligne par film             -> analyse et cold start
-- Aucune valeur NULL en sortie : age manquant remplace par la moyenne (indicateur age_missing),
-- absence d activite remplacee par 0.
-- ============================================================
USE streaming;

-- ---------------------------------------------------------- ml_ratings
DROP TABLE IF EXISTS ml_ratings;
CREATE EXTERNAL TABLE ml_ratings (user_id INT, movie_id INT, rating DOUBLE)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/streaming/warehouse/ml/ratings';
INSERT OVERWRITE TABLE ml_ratings
SELECT user_id, movie_id, rating FROM ratings_orc;

-- ---------------------------------------------------------- ml_user_features
DROP TABLE IF EXISTS ml_user_features;
CREATE EXTERNAL TABLE ml_user_features (
  user_id INT, age DOUBLE, age_missing INT, is_male INT, monthly_price DOUBLE,
  nb_ratings BIGINT, avg_rating DOUBLE,
  nb_views BIGINT, total_minutes BIGINT, avg_minutes_per_view DOUBLE, nb_distinct_movies BIGINT,
  share_mobile DOUBLE, share_tv DOUBLE, share_web DOUBLE, share_tablet DOUBLE,
  nb_tags BIGINT, nb_events BIGINT,
  churned INT)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/streaming/warehouse/ml/user_features';

INSERT OVERWRITE TABLE ml_user_features
SELECT
  u.user_id,
  COALESCE(CAST(u.age AS DOUBLE), a.avg_age),
  CASE WHEN u.age IS NULL THEN 1 ELSE 0 END,
  CASE WHEN u.gender = 'M' THEN 1 ELSE 0 END,
  COALESCE(s.monthly_price, 0.0),
  COALESCE(r.nb_ratings, 0),
  COALESCE(r.avg_rating, 0.0),
  COALESCE(l.nb_views, 0),
  COALESCE(l.total_minutes, 0),
  COALESCE(l.avg_minutes, 0.0),
  COALESCE(l.nb_movies, 0),
  COALESCE(l.share_mobile, 0.0),
  COALESCE(l.share_tv, 0.0),
  COALESCE(l.share_web, 0.0),
  COALESCE(l.share_tablet, 0.0),
  COALESCE(t.nb_tags, 0),
  COALESCE(e.nb_events, 0),
  CASE WHEN s.status = 'cancelled' THEN 1 ELSE 0 END
FROM users_orc u
CROSS JOIN (SELECT AVG(age) AS avg_age FROM users_orc) a
LEFT JOIN subscriptions_orc s ON u.user_id = s.user_id
LEFT JOIN (
  SELECT user_id, COUNT(*) AS nb_ratings, AVG(rating) AS avg_rating
  FROM ratings_orc GROUP BY user_id) r ON u.user_id = r.user_id
LEFT JOIN (
  SELECT user_id,
         COUNT(*) AS nb_views,
         SUM(watch_duration_min) AS total_minutes,
         AVG(watch_duration_min) AS avg_minutes,
         COUNT(DISTINCT movie_id) AS nb_movies,
         SUM(CASE WHEN device = 'mobile' THEN 1 ELSE 0 END) / COUNT(*) AS share_mobile,
         SUM(CASE WHEN device = 'tv' THEN 1 ELSE 0 END) / COUNT(*) AS share_tv,
         SUM(CASE WHEN device = 'web' THEN 1 ELSE 0 END) / COUNT(*) AS share_web,
         SUM(CASE WHEN device = 'tablet' THEN 1 ELSE 0 END) / COUNT(*) AS share_tablet
  FROM watch_logs_orc GROUP BY user_id) l ON u.user_id = l.user_id
LEFT JOIN (
  SELECT user_id, COUNT(*) AS nb_tags FROM tags_orc GROUP BY user_id) t ON u.user_id = t.user_id
LEFT JOIN (
  SELECT user_id, COUNT(*) AS nb_events FROM events_orc GROUP BY user_id) e ON u.user_id = e.user_id;

-- ---------------------------------------------------------- ml_movie_features
DROP TABLE IF EXISTS ml_movie_features;
CREATE EXTERNAL TABLE ml_movie_features (
  movie_id INT, title STRING, genres STRING, `year` INT,
  nb_ratings BIGINT, avg_rating DOUBLE, nb_views BIGINT, total_minutes BIGINT)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/streaming/warehouse/ml/movie_features';

INSERT OVERWRITE TABLE ml_movie_features
SELECT m.movie_id, m.title, m.genres, m.`year`,
       COALESCE(r.nb_ratings, 0), COALESCE(r.avg_rating, 0.0),
       COALESCE(l.nb_views, 0), COALESCE(l.total_minutes, 0)
FROM movies_orc m
LEFT JOIN (SELECT movie_id, COUNT(*) AS nb_ratings, AVG(rating) AS avg_rating
           FROM ratings_orc GROUP BY movie_id) r ON m.movie_id = r.movie_id
LEFT JOIN (SELECT movie_id, COUNT(*) AS nb_views, SUM(watch_duration_min) AS total_minutes
           FROM watch_logs_orc GROUP BY movie_id) l ON m.movie_id = l.movie_id;

SHOW TABLES;
