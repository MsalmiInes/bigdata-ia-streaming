#!/usr/bin/env python3
"""
Generateur de dataset 'Plateforme de Streaming' a plusieurs volumes.

Produit : movies, users, subscriptions, ratings, tags, watch_logs,
kafka_events (CSV) + import_mysql.sql (script d'initialisation MySQL).

Usage :
    python scripts/generate_data.py --size small
    python scripts/generate_data.py --size medium
    python scripts/generate_data.py --size large
    python scripts/generate_data.py --size small --out data/generated/small --seed 7

Aucune dependance externe (bibliotheque standard uniquement).
Les notes suivent une structure latente (preferences par genre), ce qui permet
a ALS d'apprendre quelque chose. ~1 % de lignes "sales" sont injectees
(vides, doublons, notes invalides) pour justifier le nettoyage avec Pig.
Les fichiers utilisent des fins de ligne Unix (\\n) : indispensable pour
Hadoop / Pig / Flume / Hive.
"""
import argparse
import csv
import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

# ----------------------------------------------------------------- volumes
SIZES = {
    #          users    movies   ratings    tags    watch_logs  kafka_events
    "small":  dict(users=1_000,   movies=500,   ratings=100_000,   tags=5_000,   logs=50_000,    events=20_000),
    "medium": dict(users=10_000,  movies=3_000, ratings=1_000_000, tags=50_000,  logs=500_000,   events=200_000),
    "large":  dict(users=50_000,  movies=8_000, ratings=5_000_000, tags=250_000, logs=2_500_000, events=1_000_000),
}

GENRES = ["Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary",
          "Drama", "Fantasy", "Horror", "Romance", "Sci-Fi", "Thriller"]
COUNTRIES = ["TN", "FR", "DE", "US", "CA", "MA", "DZ", "IT", "ES", "GB"]
PLANS = {"Basic": 7.99, "Standard": 12.99, "Premium": 17.99}
TAGS = ["funny", "dark", "classic", "twist", "family", "violent", "emotional",
        "visually stunning", "slow", "fast-paced", "based on a book", "cult"]
EVENT_TYPES = ["play", "pause", "like", "search"]
DEVICES = ["mobile", "tv", "web", "tablet"]
DIRTY_RATE = 0.01  # 1 % de lignes sales

START = datetime(2024, 1, 1)
END = datetime(2025, 12, 31)
SPAN = int((END - START).total_seconds())

# ------------------------------------------------------- script MySQL
MYSQL_DDL = r"""-- ============================================================
-- import_mysql.sql : initialisation de la base source (MySQL)
-- Execute automatiquement au 1er demarrage du conteneur mysql.
-- Les CSV sont montes dans /data (voir docker-compose, phase 2).
-- ============================================================
CREATE DATABASE IF NOT EXISTS streaming_db CHARACTER SET utf8mb4;
USE streaming_db;

DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS subscriptions;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS movies;

CREATE TABLE movies (
  movie_id INT PRIMARY KEY,
  title    VARCHAR(100),
  genres   VARCHAR(100),
  year     INT
);

CREATE TABLE users (
  user_id     INT PRIMARY KEY,
  name        VARCHAR(50),
  age         INT NULL,
  gender      CHAR(1),
  country     CHAR(2),
  signup_date DATE
);

CREATE TABLE subscriptions (
  subscription_id INT PRIMARY KEY,
  user_id         INT,
  plan            VARCHAR(20),
  start_date      DATE,
  end_date        DATE NULL,
  status          VARCHAR(20),
  monthly_price   DECIMAL(6,2)
);

-- Pas de cle primaire sur ratings : on garde volontairement les lignes
-- sales (doublons, notes invalides) pour les nettoyer avec Pig (phase 4).
CREATE TABLE ratings (
  user_id     INT,
  movie_id    INT,
  rating      DECIMAL(4,1) NULL,
  `timestamp` DATETIME
);

CREATE TABLE tags (
  user_id     INT,
  movie_id    INT,
  tag         VARCHAR(50),
  `timestamp` DATETIME
);

LOAD DATA INFILE '/data/movies.csv' INTO TABLE movies
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n' IGNORE 1 LINES;

LOAD DATA INFILE '/data/users.csv' INTO TABLE users
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n' IGNORE 1 LINES
  (user_id, name, @age, gender, country, signup_date)
  SET age = NULLIF(@age, '');

LOAD DATA INFILE '/data/subscriptions.csv' INTO TABLE subscriptions
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n' IGNORE 1 LINES
  (subscription_id, user_id, plan, start_date, @end_date, status, monthly_price)
  SET end_date = NULLIF(@end_date, '');

LOAD DATA INFILE '/data/ratings.csv' INTO TABLE ratings
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n' IGNORE 1 LINES
  (user_id, movie_id, @rating, `timestamp`)
  SET rating = NULLIF(@rating, '');

LOAD DATA INFILE '/data/tags.csv' INTO TABLE tags
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n' IGNORE 1 LINES;

SELECT 'movies' AS table_name, COUNT(*) AS nb FROM movies
UNION ALL SELECT 'users', COUNT(*) FROM users
UNION ALL SELECT 'subscriptions', COUNT(*) FROM subscriptions
UNION ALL SELECT 'ratings', COUNT(*) FROM ratings
UNION ALL SELECT 'tags', COUNT(*) FROM tags;
"""


def rand_ts(rng):
    return (START + timedelta(seconds=rng.randrange(SPAN))).strftime("%Y-%m-%d %H:%M:%S")


def rand_date(rng):
    return (START + timedelta(days=rng.randrange(SPAN // 86400))).strftime("%Y-%m-%d")


def writer(path, header):
    fh = open(path, "w", newline="", encoding="utf-8")
    w = csv.writer(fh, lineterminator="\n")
    w.writerow(header)
    return fh, w


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", choices=SIZES, default="small")
    ap.add_argument("--out", default=None, help="dossier de sortie (defaut: data/generated/<size>)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--no-dirty", action="store_true", help="ne pas injecter de lignes sales")
    a = ap.parse_args()

    cfg = SIZES[a.size]
    rng = random.Random(a.seed)
    out = Path(a.out) if a.out else Path("data/generated") / a.size
    out.mkdir(parents=True, exist_ok=True)
    dirty = 0.0 if a.no_dirty else DIRTY_RATE
    t0 = time.time()
    stats = {}

    # ---------------- movies
    fh, w = writer(out / "movies.csv", ["movie_id", "title", "genres", "year"])
    movie_genres = {}
    for m in range(1, cfg["movies"] + 1):
        g = rng.sample(GENRES, rng.choice([1, 2, 2, 3]))
        movie_genres[m] = g
        w.writerow([m, f"Movie {m:05d}", "|".join(g), rng.randint(1980, 2025)])
    fh.close()
    stats["movies"] = cfg["movies"]

    # ---------------- users (+ preferences latentes par genre)
    fh, w = writer(out / "users.csv", ["user_id", "name", "age", "gender", "country", "signup_date"])
    prefs = {}
    for u in range(1, cfg["users"] + 1):
        prefs[u] = {g: rng.uniform(-1.0, 1.0) for g in GENRES}
        age = rng.randint(14, 75)
        if rng.random() < dirty:
            age = ""  # valeur manquante volontaire
        w.writerow([u, f"User{u:06d}", age, rng.choice(["M", "F"]),
                    rng.choice(COUNTRIES), rand_date(rng)])
    fh.close()
    stats["users"] = cfg["users"]

    # ---------------- ratings (structure latente + bruit)
    fh, w = writer(out / "ratings.csv", ["user_id", "movie_id", "rating", "timestamp"])
    seen = set()
    user_activity = [0] * (cfg["users"] + 1)
    n = 0
    last_row = None
    max_pairs = cfg["users"] * cfg["movies"]
    target = min(cfg["ratings"], int(max_pairs * 0.5))
    while n < target:
        u = rng.randint(1, cfg["users"])
        m = rng.randint(1, cfg["movies"])
        if (u, m) in seen:
            continue
        seen.add((u, m))
        aff = sum(prefs[u][g] for g in movie_genres[m]) / len(movie_genres[m])
        rating = round(min(5.0, max(0.5, 3.2 + 1.6 * aff + rng.gauss(0, 0.5))) * 2) / 2
        row = [u, m, rating, rand_ts(rng)]
        user_activity[u] += 1
        r = rng.random()
        if r < dirty / 3:
            row[2] = rng.choice([0, 7, -1, 10])   # note invalide
        elif r < 2 * dirty / 3:
            row[2] = ""                            # note manquante
        w.writerow(row)
        if r > 1 - dirty / 3 and last_row:         # doublon
            w.writerow(last_row)
        last_row = row
        n += 1
    fh.close()
    stats["ratings"] = n
    del seen

    # ---------------- tags
    fh, w = writer(out / "tags.csv", ["user_id", "movie_id", "tag", "timestamp"])
    for _ in range(cfg["tags"]):
        w.writerow([rng.randint(1, cfg["users"]), rng.randint(1, cfg["movies"]),
                    rng.choice(TAGS), rand_ts(rng)])
    fh.close()
    stats["tags"] = cfg["tags"]

    # ---------------- subscriptions (churn lie a l'activite)
    fh, w = writer(out / "subscriptions.csv",
                   ["subscription_id", "user_id", "plan", "start_date", "end_date",
                    "status", "monthly_price"])
    avg_act = max(1.0, sum(user_activity) / cfg["users"])
    churned = 0
    for u in range(1, cfg["users"] + 1):
        plan = rng.choices(list(PLANS), weights=[3, 4, 2])[0]
        start = rand_date(rng)
        p_churn = min(0.8, max(0.05, 0.25 - 0.15 * (user_activity[u] / avg_act - 1)))
        if rng.random() < p_churn:
            status, churned = "cancelled", churned + 1
            end = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=rng.randint(30, 400))).strftime("%Y-%m-%d")
        else:
            status, end = "active", ""
        w.writerow([u, u, plan, start, end, status, PLANS[plan]])
    fh.close()
    stats["subscriptions"] = cfg["users"]
    stats["churn_rate"] = round(churned / cfg["users"], 3)

    # ---------------- watch_logs (-> Flume)
    fh, w = writer(out / "watch_logs.csv",
                   ["log_id", "user_id", "movie_id", "watch_duration_min", "device", "timestamp"])
    for i in range(1, cfg["logs"] + 1):
        dur = rng.randint(1, 180)
        if rng.random() < dirty:
            dur = -dur                              # duree negative = sale
        w.writerow([i, rng.randint(1, cfg["users"]), rng.randint(1, cfg["movies"]),
                    dur, rng.choice(DEVICES), rand_ts(rng)])
    fh.close()
    stats["watch_logs"] = cfg["logs"]

    # ---------------- kafka_events (-> Kafka)
    fh, w = writer(out / "kafka_events.csv", ["user_id", "movie_id", "event_type", "timestamp"])
    for _ in range(cfg["events"]):
        w.writerow([rng.randint(1, cfg["users"]), rng.randint(1, cfg["movies"]),
                    rng.choices(EVENT_TYPES, weights=[6, 3, 1, 2])[0], rand_ts(rng)])
    fh.close()
    stats["kafka_events"] = cfg["events"]

    # ---------------- import_mysql.sql
    (out / "import_mysql.sql").write_text(MYSQL_DDL, encoding="utf-8", newline="\n")

    meta = {"size": a.size, "seed": a.seed, "dirty_rate": dirty,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "rows": stats, "duration_s": round(time.time() - t0, 1)}
    (out / "_metadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(json.dumps(meta, indent=2))
    print(f"\nDataset '{a.size}' ecrit dans : {out.resolve()}")


if __name__ == "__main__":
    main()
