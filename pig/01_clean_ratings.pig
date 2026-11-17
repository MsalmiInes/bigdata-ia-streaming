-- ============================================================
-- 01_clean_ratings.pig : nettoyage de la table ratings
--   entree  : /streaming/raw/ratings/ratings.csv   (avec en-tete)
--   sortie  : /streaming/clean/ratings             (lignes valides, sans doublon)
--             /streaming/rejects/ratings           (lignes rejetees + motif)
-- Regles : en-tete retire, note vide ou hors [0.5 ; 5] rejetee, doublons supprimes
-- ============================================================
SET job.name 'pig-01-clean-ratings';
SET default_parallel 2;

-- 1) LECTURE : tout en texte pour ne pas faire echouer le chargement sur une valeur sale
ratings_raw = LOAD '/streaming/raw/ratings/ratings.csv' USING PigStorage(',')
              AS (user_id:chararray, movie_id:chararray, rating:chararray, ts:chararray);

-- 2) EN-TETE : on retire la 1ere ligne (user_id,movie_id,rating,timestamp)
ratings_body = FILTER ratings_raw BY user_id != 'user_id';

-- 3) TYPAGE : une valeur impossible a convertir (ex. note vide) devient NULL
ratings_typed = FOREACH ratings_body GENERATE
                (int)user_id AS user_id,
                (int)movie_id AS movie_id,
                (double)rating AS rating,
                ts;

-- 4) VALIDATION : valides d'un cote, tout le reste (OTHERWISE) en rejets
SPLIT ratings_typed INTO
      ratings_ok  IF (user_id IS NOT NULL AND movie_id IS NOT NULL AND ts IS NOT NULL
                      AND rating IS NOT NULL AND rating >= 0.5 AND rating <= 5.0),
      ratings_bad OTHERWISE;

-- 5) DOUBLONS : lignes strictement identiques
ratings_clean = DISTINCT ratings_ok;

-- 6) REJETS avec un motif (utile pour le rapport qualite)
ratings_rej = FOREACH ratings_bad GENERATE user_id, movie_id, rating, ts,
              'note_manquante_ou_invalide' AS reason;

-- 7) ECRITURE (on supprime d'abord l'ancienne sortie pour pouvoir relancer le script)
fs -rm -r -f /streaming/clean/ratings;
fs -rm -r -f /streaming/rejects/ratings;
STORE ratings_clean INTO '/streaming/clean/ratings' USING PigStorage(',');
STORE ratings_rej   INTO '/streaming/rejects/ratings' USING PigStorage(',');
