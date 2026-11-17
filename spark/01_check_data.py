"""
Phase 6 - etape 1 : Spark lit les tables de features produites par Hive (ORC dans HDFS)
et verifie leur contenu. Prouve aussi que le calcul est distribue sur le cluster.
"""
import sys

from pyspark.sql import functions as F

from common import banner, cluster_info, get_spark, read_ml, save_json, say

RATINGS = ["user_id", "movie_id", "rating"]
USERS = ["user_id", "age", "age_missing", "is_male", "monthly_price", "nb_ratings", "avg_rating",
         "nb_views", "total_minutes", "avg_minutes_per_view", "nb_distinct_movies",
         "share_mobile", "share_tv", "share_web", "share_tablet", "nb_tags", "nb_events", "churned"]
MOVIES = ["movie_id", "title", "genres", "year", "nb_ratings", "avg_rating", "nb_views", "total_minutes"]

spark = get_spark("phase6-01-check-data")
banner("1) Lecture des tables Hive (ORC) depuis Spark")
ratings = read_ml(spark, "ratings", RATINGS)
users = read_ml(spark, "user_features", USERS)
movies = read_ml(spark, "movie_features", MOVIES)

n_ratings, n_users, n_movies = ratings.count(), users.count(), movies.count()
info = cluster_info(spark)
say(f"ml_ratings          : {n_ratings:>9} lignes")
say(f"ml_user_features    : {n_users:>9} lignes, {len(USERS)} colonnes")
say(f"ml_movie_features   : {n_movies:>9} lignes, {len(MOVIES)} colonnes")
say(f"Spark {info['spark_version']} | master {info['master']} | executeurs : {info['executors']} "
    f"| parallelisme : {info['default_parallelism']}")

banner("2) Qualite et description de la matrice utilisateur x film")
stats = ratings.agg(
    F.countDistinct("user_id").alias("users"), F.countDistinct("movie_id").alias("movies"),
    F.min("rating").alias("min"), F.max("rating").alias("max"), F.avg("rating").alias("avg"),
    F.stddev("rating").alias("std"),
    F.sum(F.col("rating").isNull().cast("int")).alias("nulls")).first()
density = n_ratings / (stats["users"] * stats["movies"])
say(f"utilisateurs ayant note : {stats['users']} | films notes : {stats['movies']}")
say(f"note : min {stats['min']} | max {stats['max']} | moyenne {stats['avg']:.3f} | ecart-type {stats['std']:.3f}")
say(f"densite de la matrice : {100 * density:.1f} % ({n_ratings} notes sur {stats['users'] * stats['movies']} cases)")
say("\nRepartition des notes :")
ratings.groupBy("rating").count().orderBy("rating").show(10, False)

null_cells = users.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in USERS]).first()
n_null_cells = int(sum(v or 0 for v in null_cells))
churn_rate = users.agg(F.avg("churned")).first()[0]
say(f"valeurs NULL dans ml_user_features : {n_null_cells} | taux de churn : {100 * churn_rate:.1f} %")

checks = {
    "ratings non vide": n_ratings > 0,
    "aucune note NULL": stats["nulls"] == 0,
    "notes dans [0.5 ; 5]": stats["min"] >= 0.5 and stats["max"] <= 5.0,
    "aucun NULL dans les features utilisateur": n_null_cells == 0,
    "calcul distribue (au moins 1 executeur)": info["executors"] >= 1 or info["master"].startswith("local"),
}
banner("3) Controles")
for k, v in checks.items():
    say(f"  [{'OK' if v else 'KO'}] {k}")

save_json("data_check.json", {
    **info, "n_ratings": n_ratings, "n_users": n_users, "n_movies": n_movies,
    "users_with_ratings": stats["users"], "movies_with_ratings": stats["movies"],
    "density": density, "rating_mean": stats["avg"], "rating_std": stats["std"],
    "churn_rate": churn_rate, "checks": checks})
spark.stop()
sys.exit(0 if all(checks.values()) else 1)
