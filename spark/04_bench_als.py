"""
Phase 8 - benchmark d'ALS : temps d'entrainement selon le volume et le nombre de workers Spark.

  - lit ml_ratings (ORC, ecrit par Hive) dans HDFS : ML_BASE=hdfs://namenode:9000/bench/<size>/warehouse/ml
  - decoupe 80/20, met en cache, puis :
      1 entrainement d'echauffement (2 iterations, non compte : demarrage des executeurs, JIT)
      REPS entrainements chronometres (parametres fixes : meme travail a chaque fois)
      RMSE sur le jeu de test, et temps de generation du top-5 de tous les utilisateurs
  - la configuration (cores / memoire des executeurs) est fixee ici : meme demande pour 1 et pour 2 workers,
    seul le nombre de workers disponibles change.

Usage : python3 04_bench_als.py --size small --workers 1 [--reps 2] [--no-reco]
Ecrit results/bench_als_<size>_<workers>w.json
"""
import argparse
import os
import statistics
import sys
import time

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import HDFS_OUT, banner, cluster_info, read_ml, save_json, say

ap = argparse.ArgumentParser()
ap.add_argument("--size", required=True)
ap.add_argument("--workers", type=int, required=True, help="nombre de workers Spark disponibles (etiquette)")
ap.add_argument("--reps", type=int, default=3)
ap.add_argument("--rank", type=int, default=12)
ap.add_argument("--reg", type=float, default=0.1)
ap.add_argument("--iters", type=int, default=10)
ap.add_argument("--cores-max", type=int, default=4, help="cores demandes au total (identique pour 1 et 2 workers)")
ap.add_argument("--executor-cores", type=int, default=2)
ap.add_argument("--executor-memory", default="1g")
ap.add_argument("--no-reco", action="store_true", help="ne pas mesurer recommendForAllUsers (gros volumes)")
ap.add_argument("--seed", type=int, default=42)
args = ap.parse_args()

builder = (SparkSession.builder.appName(f"bench-als-{args.size}-{args.workers}w")
           .config("spark.ui.showConsoleProgress", "false")
           .config("spark.cores.max", str(args.cores_max))
           .config("spark.executor.cores", str(args.executor_cores))
           .config("spark.executor.memory", args.executor_memory))
if os.environ.get("SPARK_MASTER"):                       # uniquement pour tester hors du cluster
    builder = builder.master(os.environ["SPARK_MASTER"])
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir(f"{HDFS_OUT}/tmp/spark_checkpoints")


def make_als(iters):
    return ALS(userCol="user_id", itemCol="movie_id", ratingCol="rating", coldStartStrategy="drop",
               rank=args.rank, regParam=args.reg, maxIter=iters, seed=args.seed, checkpointInterval=5)


def fit_timed(iters):
    t0 = time.time()
    model = make_als(iters).fit(train)
    model.userFactors.count()
    model.itemFactors.count()
    return model, round(time.time() - t0, 2)


banner(f"Benchmark ALS : jeu '{args.size}', {args.workers} worker(s)")
t0 = time.time()
ratings = (read_ml(spark, "ratings", ["user_id", "movie_id", "rating"])
           .select(F.col("user_id").cast("int"), F.col("movie_id").cast("int"), F.col("rating").cast("double")))
train, test = ratings.randomSplit([0.8, 0.2], seed=args.seed)
train, test = train.cache(), test.cache()
n_train, n_test = train.count(), test.count()
read_cache_s = round(time.time() - t0, 2)
info = cluster_info(spark)
say(f"train {n_train} | test {n_test} | lecture ORC + cache : {read_cache_s} s")
say(f"Spark {info['spark_version']} | executeurs : {info['executors']} | "
    f"parallelisme (cores) : {info['default_parallelism']} | master : {info['master']}")

_, warm_s = fit_timed(2)
say(f"echauffement (2 iterations, non compte) : {warm_s} s")

fits, model = [], None
for i in range(args.reps):
    model, s = fit_timed(args.iters)
    fits.append(s)
    say(f"entrainement {i + 1}/{args.reps} ({args.iters} iterations, rank {args.rank}) : {s} s")

pred = model.transform(test)
rmse = float(RegressionEvaluator(labelCol="rating", predictionCol="prediction", metricName="rmse").evaluate(pred))
say(f"RMSE test : {rmse:.4f}")

reco_s = None
if not args.no_reco:
    t0 = time.time()
    n_rec = model.recommendForAllUsers(5).count()
    reco_s = round(time.time() - t0, 2)
    say(f"top-5 pour {n_rec} utilisateurs : {reco_s} s")

res = {"size": args.size, "workers": args.workers, "executors": info["executors"],
       "default_parallelism": info["default_parallelism"], "master": info["master"],
       "spark_version": info["spark_version"], "cores_max": args.cores_max,
       "executor_cores": args.executor_cores, "executor_memory": args.executor_memory,
       "n_train": n_train, "n_test": n_test, "read_cache_s": read_cache_s, "warmup_s": warm_s,
       "params": {"rank": args.rank, "regParam": args.reg, "maxIter": args.iters}, "reps": args.reps,
       "fit_s_list": fits, "fit_s_mean": round(statistics.mean(fits), 2), "fit_s_min": min(fits),
       "rmse": rmse, "reco_s": reco_s}
save_json(f"bench_als_{args.size}_{args.workers}w.json", res)
spark.stop()
