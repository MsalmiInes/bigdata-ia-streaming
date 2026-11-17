-- ============================================================
-- 04_transform.pig : jointures et agregations (donnees propres -> tables pour l'IA)
--   ratings_enriched : chaque note + titre / genres / annee du film  (JOIN)
--   movie_stats      : par film  -> nb de notes, note moyenne, nb de vues, minutes
--   user_features    : par utilisateur -> profil + abonnement + activite + etiquette "churned"
--   Une valeur vide dans les sorties = NULL (ex. film jamais note).
-- ============================================================
SET job.name 'pig-04-transform';
SET default_parallel 2;

-- ---------------------------------------------------------- lecture des donnees propres
ratings = LOAD '/streaming/clean/ratings' USING PigStorage(',')
          AS (user_id:int, movie_id:int, rating:double, ts:chararray);
movies  = LOAD '/streaming/clean/movies' USING PigStorage(',')
          AS (movie_id:int, title:chararray, genres:chararray, year:int);
users   = LOAD '/streaming/clean/users' USING PigStorage(',')
          AS (user_id:int, name:chararray, age:int, gender:chararray, country:chararray,
              signup_date:chararray);
subs    = LOAD '/streaming/clean/subscriptions' USING PigStorage(',')
          AS (subscription_id:int, user_id:int, plan:chararray, start_date:chararray,
              end_date:chararray, status:chararray, monthly_price:double);
logs    = LOAD '/streaming/clean/watch_logs' USING PigStorage(',')
          AS (log_id:long, user_id:int, movie_id:int, watch_duration_min:int,
              device:chararray, ts:chararray);

-- ============================================================ 1) ratings_enriched
j_rm = JOIN ratings BY movie_id, movies BY movie_id;
ratings_enriched = FOREACH j_rm GENERATE
                   ratings::user_id AS user_id,
                   ratings::movie_id AS movie_id,
                   ratings::rating AS rating,
                   ratings::ts AS ts,
                   movies::title AS title,
                   movies::genres AS genres,
                   movies::year AS year;
fs -rm -r -f /streaming/features/ratings_enriched;
STORE ratings_enriched INTO '/streaming/features/ratings_enriched' USING PigStorage(',');

-- ============================================================ 2) movie_stats
g_rm = GROUP ratings BY movie_id;
rm_agg = FOREACH g_rm GENERATE group AS movie_id,
         COUNT(ratings) AS nb_ratings,
         AVG(ratings.rating) AS avg_rating;
g_lm = GROUP logs BY movie_id;
lm_agg = FOREACH g_lm GENERATE group AS movie_id,
         COUNT(logs) AS nb_views,
         SUM(logs.watch_duration_min) AS total_minutes;

j_m1 = JOIN movies BY movie_id LEFT OUTER, rm_agg BY movie_id;
m1 = FOREACH j_m1 GENERATE
     movies::movie_id AS movie_id, movies::title AS title, movies::genres AS genres,
     movies::year AS year, rm_agg::nb_ratings AS nb_ratings, rm_agg::avg_rating AS avg_rating;
j_m2 = JOIN m1 BY movie_id LEFT OUTER, lm_agg BY movie_id;
movie_stats = FOREACH j_m2 GENERATE
              m1::movie_id AS movie_id, m1::title AS title, m1::genres AS genres,
              m1::year AS year, m1::nb_ratings AS nb_ratings, m1::avg_rating AS avg_rating,
              lm_agg::nb_views AS nb_views, lm_agg::total_minutes AS total_minutes;
fs -rm -r -f /streaming/features/movie_stats;
STORE movie_stats INTO '/streaming/features/movie_stats' USING PigStorage(',');

-- ============================================================ 3) user_features
g_ur = GROUP ratings BY user_id;
ur_agg = FOREACH g_ur GENERATE group AS user_id,
         COUNT(ratings) AS nb_ratings,
         AVG(ratings.rating) AS avg_rating;
g_ul = GROUP logs BY user_id;
ul_agg = FOREACH g_ul GENERATE group AS user_id,
         COUNT(logs) AS nb_views,
         SUM(logs.watch_duration_min) AS total_minutes;

j_u1 = JOIN users BY user_id LEFT OUTER, subs BY user_id;
u1 = FOREACH j_u1 GENERATE
     users::user_id AS user_id, users::age AS age, users::gender AS gender,
     users::country AS country, subs::plan AS plan, subs::monthly_price AS monthly_price,
     ((subs::status == 'cancelled') ? 1 : 0) AS churned;
j_u2 = JOIN u1 BY user_id LEFT OUTER, ur_agg BY user_id;
u2 = FOREACH j_u2 GENERATE
     u1::user_id AS user_id, u1::age AS age, u1::gender AS gender, u1::country AS country,
     u1::plan AS plan, u1::monthly_price AS monthly_price, u1::churned AS churned,
     ur_agg::nb_ratings AS nb_ratings, ur_agg::avg_rating AS avg_rating;
j_u3 = JOIN u2 BY user_id LEFT OUTER, ul_agg BY user_id;
user_features = FOREACH j_u3 GENERATE
                u2::user_id AS user_id, u2::age AS age, u2::gender AS gender,
                u2::country AS country, u2::plan AS plan, u2::monthly_price AS monthly_price,
                u2::churned AS churned, u2::nb_ratings AS nb_ratings, u2::avg_rating AS avg_rating,
                ul_agg::nb_views AS nb_views, ul_agg::total_minutes AS total_minutes;
fs -rm -r -f /streaming/features/user_features;
STORE user_features INTO '/streaming/features/user_features' USING PigStorage(',');
