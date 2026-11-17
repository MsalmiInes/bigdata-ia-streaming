-- ============================================================
-- 02_clean_reference.pig : nettoyage des 4 petites tables
--   movies, users, subscriptions, tags
--   sorties : /streaming/clean/<table>  et  /streaming/rejects/<table>
-- ============================================================
SET job.name 'pig-02-clean-reference';
SET default_parallel 2;

-- ---------------------------------------------------------- MOVIES
mv_raw = LOAD '/streaming/raw/movies/movies.csv' USING PigStorage(',')
         AS (movie_id:chararray, title:chararray, genres:chararray, year:chararray);
mv_body = FILTER mv_raw BY movie_id != 'movie_id';
mv_typed = FOREACH mv_body GENERATE (int)movie_id AS movie_id, TRIM(title) AS title,
           genres, (int)year AS year;
SPLIT mv_typed INTO
      mv_ok  IF (movie_id IS NOT NULL AND year IS NOT NULL AND year >= 1900 AND year <= 2030),
      mv_bad OTHERWISE;
mv_clean = DISTINCT mv_ok;
mv_rej = FOREACH mv_bad GENERATE movie_id, title, genres, year, 'film_invalide' AS reason;
fs -rm -r -f /streaming/clean/movies;
fs -rm -r -f /streaming/rejects/movies;
STORE mv_clean INTO '/streaming/clean/movies' USING PigStorage(',');
STORE mv_rej   INTO '/streaming/rejects/movies' USING PigStorage(',');

-- ---------------------------------------------------------- USERS
-- Un age manquant est CONSERVE (age NULL) : on le traitera en Hive / Spark.
us_raw = LOAD '/streaming/raw/users/users.csv' USING PigStorage(',')
         AS (user_id:chararray, name:chararray, age:chararray, gender:chararray,
             country:chararray, signup_date:chararray);
us_body = FILTER us_raw BY user_id != 'user_id';
us_typed = FOREACH us_body GENERATE
           (int)user_id AS user_id,
           name,
           (int)age AS age,
           ((gender == 'M' OR gender == 'F') ? gender : 'U') AS gender,
           UPPER(TRIM(country)) AS country,
           signup_date;
SPLIT us_typed INTO
      us_ok  IF (user_id IS NOT NULL AND (age IS NULL OR (age >= 10 AND age <= 100))),
      us_bad OTHERWISE;
us_clean = DISTINCT us_ok;
us_rej = FOREACH us_bad GENERATE user_id, name, age, gender, country, signup_date,
         'utilisateur_invalide' AS reason;
fs -rm -r -f /streaming/clean/users;
fs -rm -r -f /streaming/rejects/users;
STORE us_clean INTO '/streaming/clean/users' USING PigStorage(',');
STORE us_rej   INTO '/streaming/rejects/users' USING PigStorage(',');

-- ---------------------------------------------------------- SUBSCRIPTIONS
-- end_date est vide pour les abonnements actifs : c'est normal (NULL)
sb_raw = LOAD '/streaming/raw/subscriptions/subscriptions.csv' USING PigStorage(',')
         AS (subscription_id:chararray, user_id:chararray, plan:chararray, start_date:chararray,
             end_date:chararray, status:chararray, monthly_price:chararray);
sb_body = FILTER sb_raw BY subscription_id != 'subscription_id';
sb_typed = FOREACH sb_body GENERATE
           (int)subscription_id AS subscription_id,
           (int)user_id AS user_id,
           plan, start_date, end_date,
           LOWER(TRIM(status)) AS status,
           (double)monthly_price AS monthly_price;
SPLIT sb_typed INTO
      sb_ok  IF (subscription_id IS NOT NULL AND user_id IS NOT NULL AND monthly_price IS NOT NULL
                 AND monthly_price > 0.0 AND (status == 'active' OR status == 'cancelled')),
      sb_bad OTHERWISE;
sb_clean = DISTINCT sb_ok;
sb_rej = FOREACH sb_bad GENERATE subscription_id, user_id, plan, start_date, end_date, status,
         monthly_price, 'abonnement_invalide' AS reason;
fs -rm -r -f /streaming/clean/subscriptions;
fs -rm -r -f /streaming/rejects/subscriptions;
STORE sb_clean INTO '/streaming/clean/subscriptions' USING PigStorage(',');
STORE sb_rej   INTO '/streaming/rejects/subscriptions' USING PigStorage(',');

-- ---------------------------------------------------------- TAGS
tg_raw = LOAD '/streaming/raw/tags/tags.csv' USING PigStorage(',')
         AS (user_id:chararray, movie_id:chararray, tag:chararray, ts:chararray);
tg_body = FILTER tg_raw BY user_id != 'user_id';
tg_typed = FOREACH tg_body GENERATE (int)user_id AS user_id, (int)movie_id AS movie_id,
           LOWER(TRIM(tag)) AS tag, ts;
SPLIT tg_typed INTO
      tg_ok  IF (user_id IS NOT NULL AND movie_id IS NOT NULL AND tag IS NOT NULL),
      tg_bad OTHERWISE;
tg_clean = DISTINCT tg_ok;
tg_rej = FOREACH tg_bad GENERATE user_id, movie_id, tag, ts, 'tag_invalide' AS reason;
fs -rm -r -f /streaming/clean/tags;
fs -rm -r -f /streaming/rejects/tags;
STORE tg_clean INTO '/streaming/clean/tags' USING PigStorage(',');
STORE tg_rej   INTO '/streaming/rejects/tags' USING PigStorage(',');
