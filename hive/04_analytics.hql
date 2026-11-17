-- ============================================================
-- 04_analytics.hql : requetes analytiques (sur les tables ORC)
-- ============================================================
USE streaming;

SELECT 'Q1 - Top 10 des films les mieux notes (au moins 50 notes)' AS requete;
SELECT m.movie_id, m.title, m.genres, COUNT(*) AS nb_notes, ROUND(AVG(r.rating), 2) AS note_moyenne
FROM ratings_orc r JOIN movies_orc m ON r.movie_id = m.movie_id
GROUP BY m.movie_id, m.title, m.genres
HAVING COUNT(*) >= 50
ORDER BY note_moyenne DESC, nb_notes DESC
LIMIT 10;

SELECT 'Q2 - Note moyenne par genre (un film a 1 a 3 genres)' AS requete;
SELECT mg.genre, COUNT(DISTINCT mg.movie_id) AS nb_films, COUNT(*) AS nb_notes,
       ROUND(AVG(r.rating), 2) AS note_moyenne
FROM ratings_orc r
JOIN (SELECT movie_id, genre
      FROM movies_orc
      LATERAL VIEW explode(split(genres, '\\|')) g AS genre) mg
  ON r.movie_id = mg.movie_id
GROUP BY mg.genre
ORDER BY note_moyenne DESC;

SELECT 'Q3 - Activite par tranche d age' AS requete;
SELECT tranche_age, COUNT(*) AS nb_utilisateurs,
       ROUND(AVG(nb_ratings), 1) AS notes_par_user,
       ROUND(AVG(avg_rating), 2) AS note_moyenne,
       ROUND(AVG(total_minutes), 0) AS minutes_visionnees
FROM (
  SELECT CASE WHEN u.age IS NULL THEN 'inconnu'
              WHEN u.age < 18 THEN '14-17'
              WHEN u.age < 25 THEN '18-24'
              WHEN u.age < 35 THEN '25-34'
              WHEN u.age < 50 THEN '35-49'
              ELSE '50+' END AS tranche_age,
         f.nb_ratings, f.avg_rating, f.total_minutes
  FROM users_orc u JOIN ml_user_features f ON u.user_id = f.user_id
) t
GROUP BY tranche_age
ORDER BY tranche_age;

SELECT 'Q4 - Taux de resiliation par formule d abonnement' AS requete;
SELECT plan, COUNT(*) AS nb_abonnes,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS nb_resilies,
       ROUND(100 * SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*), 1) AS taux_resiliation_pct,
       MAX(monthly_price) AS prix_mensuel
FROM subscriptions_orc
GROUP BY plan
ORDER BY prix_mensuel;

SELECT 'Q5 - Visionnage par appareil' AS requete;
SELECT device, COUNT(*) AS nb_vues, ROUND(AVG(watch_duration_min), 1) AS duree_moyenne_min,
       SUM(watch_duration_min) AS total_minutes
FROM watch_logs_orc
GROUP BY device
ORDER BY nb_vues DESC;

SELECT 'Q6 - Evolution mensuelle des notes' AS requete;
SELECT date_format(ts, 'yyyy-MM') AS mois, COUNT(*) AS nb_notes, ROUND(AVG(rating), 2) AS note_moyenne
FROM ratings_orc
GROUP BY date_format(ts, 'yyyy-MM')
ORDER BY mois;

SELECT 'Q7 - Repartition des evenements temps reel' AS requete;
SELECT event_type, COUNT(*) AS nb, ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pourcentage
FROM events_orc
GROUP BY event_type
ORDER BY nb DESC;

SELECT 'Q8 - Resiliation selon le nombre de notes (justifie le modele de churn)' AS requete;
SELECT tranche_notes, COUNT(*) AS nb_utilisateurs, SUM(churned) AS nb_resilies,
       ROUND(100 * SUM(churned) / COUNT(*), 1) AS taux_resiliation_pct
FROM (
  SELECT CASE WHEN nb_ratings < 80 THEN 'a: moins de 80'
              WHEN nb_ratings < 100 THEN 'b: 80 a 99'
              WHEN nb_ratings < 120 THEN 'c: 100 a 119'
              ELSE 'd: 120 et plus' END AS tranche_notes,
         churned
  FROM ml_user_features
) t
GROUP BY tranche_notes
ORDER BY tranche_notes;
