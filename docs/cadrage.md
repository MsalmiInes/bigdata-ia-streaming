# Cadrage du projet : Big Data avancé au service de l'IA

## 1. Titre
Plateforme de streaming intelligente : recommandation de contenus par IA
sur une architecture Big Data (Sqoop, Flume, Kafka, Pig, Hive, Spark MLlib).

## 2. Problématique
Comment un écosystème Big Data (Pig, Hive, Spark) permet-il de préparer des
données massives et hétérogènes (relationnelles, logs, événements temps réel)
puis d'entraîner et d'exploiter un modèle d'IA de recommandation, avec de
meilleures performances et une meilleure scalabilité qu'un traitement classique ?

## 3. Objectifs
- O1 : Ingérer les trois types de sources vers HDFS (Sqoop, Flume, Kafka).
- O2 : Nettoyer et transformer les données avec Pig.
- O3 : Construire un entrepôt analytique et des features avec Hive.
- O4 : Entraîner et évaluer un modèle ALS avec Spark MLlib (+ churn optionnel).
- O5 : Mesurer l'apport du Big Data (temps, volume, scalabilité).
- O6 : Étendre au quasi temps réel (Kafka + Spark Structured Streaming), optionnel.

## 4. Cas d'usage IA
- Principal : recommandation de films (filtrage collaboratif ALS).
- Secondaire : prédiction du désabonnement (Random Forest).

## 5. Métriques de succès
| Domaine | Métrique | Objectif |
|---|---|---|
| Recommandation | RMSE sur jeu de test | Inférieur au RMSE d'une baseline (note moyenne) |
| Recommandation | Precision@5 | Supérieure à une recommandation aléatoire / « films populaires » |
| Churn (option) | AUC-ROC | Supérieure ou égale à 0,75 |
| Qualité des données | % de lignes rejetées par Pig | Mesuré et documenté |
| Performance | Temps de chaque étape (s) pour small / medium / large | Tableau comparatif |
| Stockage | Taille CSV vs ORC/Parquet | Tableau comparatif |
| Scalabilité | Entraînement ALS avec 1 puis 2 workers | Gain mesuré |

## 6. Dataset
Données synthétiques générées par `scripts/generate_data.py` (graine fixe,
donc reproductibles) : 7 fichiers CSV + script `import_mysql.sql`.

| Jeu | Utilisateurs | Films | Notes | Logs | Événements | Usage |
|---|---|---|---|---|---|---|
| small | 1 000 | 500 | 100 000 | 50 000 | 20 000 | Développement et tests |
| medium | 10 000 | 3 000 | 1 000 000 | 500 000 | 200 000 | Démonstration |
| large | 50 000 | 8 000 | 5 000 000 | 2 500 000 | 1 000 000 | Benchmark (phase 8) |

| Fichier | Contenu | Destination |
|---|---|---|
| movies, users, subscriptions, ratings, tags | Données relationnelles | MySQL, puis HDFS via Sqoop |
| watch_logs | Logs de visionnage | HDFS via Flume |
| kafka_events | Événements temps réel (play, pause, like, search) | Topic Kafka `streaming.events` |

Environ 1 % de lignes sales sont injectées (valeurs vides, doublons, notes
invalides, durées négatives) pour justifier le nettoyage avec Pig.

## 7. Périmètre et hypothèses
- Cluster simulé avec Docker sur un seul PC (Windows 11 + WSL2).
- Hors périmètre : déploiement cloud, haute disponibilité réelle.

## 8. Planning
Plan en 9 phases : cadrage, Docker, ingestion, Pig, Hive, Spark,
streaming, évaluation, livrables.
