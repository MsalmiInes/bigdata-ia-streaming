# Benchmark : apport du Big Data (phase 8)

Toutes les valeurs viennent des mesures de `bench/run.sh` ; "-" = non mesuré ou échec. Cluster simulé avec Docker sur un seul PC (Windows 11 + WSL2, 6 CPU, 8 Go).

## 1. Volumes de données

| Jeu | Lignes (7 tables) | Notes | CSV (Mo) |
|---|---|---|---|
| small | 177 886 | 100 386 | 6.0 |
| medium | 1 776 312 | 1 003 312 | 62.9 |
| large | 8 874 697 | 5 016 697 | 325.6 |

## 2. Temps d'exécution par étape (secondes)

| Étape | small | medium | large |
|---|---|---|---|
| Chargement des CSV dans HDFS | 75.0 | 64.6 | 90.9 |
| Pig 01 : clean_ratings | 567.7 | 566.4 | 626.9 |
| Pig 02 : clean_reference | 2 155.1 | 2 220.9 | 2 255.9 |
| Pig 03 : clean_logs_events | 3 560.5 | 1 092.6 | 1 161.2 |
| Pig 04 : transform | 1 975.5 | 1 889.6 | 2 038.2 |
| **Pig : total (4 scripts)** | 8 258.8 | 5 769.5 | 6 082.2 |
| Hive 01 : tables_csv | 9.0 | 5.7 | 8.9 |
| Hive 02 : tables_orc_parquet | 66.2 | 39.1 | 115.5 |
| Hive 03 : features_ml | 36.7 | 23.7 | 76.0 |
| **Hive : total (3 scripts)** | 111.9 | 68.5 | 200.4 |

### Débit et croissance

| Indicateur | small | medium | large |
|---|---|---|---|
| Pig : lignes traitées par seconde | 22 | 308 | 1 459 |
| Hive : lignes traitées par seconde | 1 590 | 25 932 | 44 285 |

Croissance entre jeux consécutifs (volume ×V, temps ×T ; T < V signifie que le temps fixe de démarrage des jobs pèse moins quand le volume augmente) :

| Passage | Outil | Volume | Temps |
|---|---|---|---|
| small → medium | Pig | ×10.0 | ×0.7 |
| small → medium | Hive | ×10.0 | ×0.6 |
| medium → large | Pig | ×5.0 | ×1.1 |
| medium → large | Hive | ×5.0 | ×2.9 |

## 3. Formats de stockage : CSV, ORC, Parquet

Taille sur disque de la table `ratings` (une copie dans HDFS) :

| Jeu | CSV (Mo) | ORC (Mo) | Parquet (Mo) | CSV / ORC | CSV / Parquet |
|---|---|---|---|---|---|
| small | 3.00 | 0.60 | 0.99 | ×5.0 | ×3.0 |
| medium | 31.75 | 6.24 | 9.02 | ×5.1 | ×3.5 |
| large | 164.07 | 32.16 | 45.14 | ×5.1 | ×3.6 |

Temps de requête Hive sur `ratings` (moyenne de plusieurs exécutions, durée de connexion beeline retirée) :

| Jeu | Format / cas | A : filtre + moyenne (s) | B : regroupement par film (s) |
|---|---|---|---|
| small | csv | 0.0 | 1.4 |
| small | orc | 0.0 | 1.0 |
| small | parquet | 0.3 | 1.5 |
| small | ORC, filtre sur la partition (année) | 0.5 | - |
| small | ORC, même filtre sans la partition | 0.0 | - |
| medium | csv | 3.3 | 3.9 |
| medium | orc | 1.8 | 3.2 |
| medium | parquet | 1.6 | 3.1 |
| medium | ORC, filtre sur la partition (année) | 3.3 | - |
| medium | ORC, même filtre sans la partition | 3.2 | - |
| large | csv | 4.9 | 6.3 |
| large | orc | 2.9 | 2.2 |
| large | parquet | 3.1 | 3.7 |
| large | ORC, filtre sur la partition (année) | 1.5 | - |
| large | ORC, même filtre sans la partition | 2.9 | - |

## 4. Spark ALS : 1 worker contre 2 workers

Même demande à chaque exécution (4 cœurs demandés, 2 par exécuteur, 1 Go chacun, 10 itérations, rang 12). Avec 1 worker Spark n'obtient que 2 cœurs, avec 2 workers il en obtient 4. Temps d'entraînement : moyenne des répétitions, après un échauffement non compté.

| Jeu | Notes (train) | 1 worker (s) | 2 workers (s) | Accélération | Efficacité | RMSE 1w | RMSE 2w | Exécuteurs 1w / 2w |
|---|---|---|---|---|---|---|---|---|
| small | 79 334 | 5.6 | 7.3 | ×0.77 | 38 % | 0.6615 | 0.6615 | 1 / 2 |
| medium | 794 971 | 10.0 | 13.7 | ×0.73 | 37 % | 0.6533 | 0.6533 | 1 / 2 |
| large | 3 973 911 | 22.9 | 35.5 | ×0.65 | 32 % | 0.6439 | 0.6439 | 1 / 2 |

Lecture ORC + mise en cache, et génération du top-5 de tous les utilisateurs :

| Jeu | Lecture 1w (s) | Lecture 2w (s) | Top-5 1w (s) | Top-5 2w (s) |
|---|---|---|---|---|
| small | 10.9 | 14.6 | 7.9 | 4.1 |
| medium | 11.2 | 12.5 | 5.4 | 6.2 |
| large | 18.9 | 21.9 | - | - |

## 5. Cluster ou une seule machine ? Pig contre pandas

Même nettoyage et mêmes transformations que les scripts Pig 01 à 04, avec pandas en mémoire. Les nombres de lignes propres sont comparés à ceux de Pig.

| Jeu | pandas (s) | Pig (s) | Pig / pandas | Mémoire max pandas (Mo) | Lignes propres pandas = Pig |
|---|---|---|---|---|---|
| small | 0.6 | 8 258.8 | ×14239 | 151 | identiques |
| medium | 6.8 | 5 769.5 | ×847 | 590 | identiques |
| large | 42.9 | 6 082.2 | ×142 | 2 504 | identiques |

## 6. Lecture des résultats

- **Pig, small → medium** : le volume est multiplié par 10.0, le temps par 0.7. Le temps croît moins vite que le volume : le coût fixe de démarrage des jobs MapReduce domine encore.
- **Hive, small → medium** : volume ×10.0, temps ×0.6.
- **Pig, medium → large** : le volume est multiplié par 5.0, le temps par 1.1. Le temps croît moins vite que le volume : le coût fixe de démarrage des jobs MapReduce domine encore.
- **Hive, medium → large** : volume ×5.0, temps ×2.9.
- **small** : pandas traite le même nettoyage 14239 fois plus vite que Pig (0.6 s contre 8259 s). À ce volume une seule machine suffit : le cluster n'apporte pas de gain de vitesse.
- **ALS, small** : 2 workers donnent une accélération de ×0.77 (efficacité 38 %). Le gain reste limité : les 2 workers partagent les mêmes CPU et le même disque, et les échanges entre exécuteurs coûtent du temps.
- **Stockage, small** : ORC occupe 5.0 fois moins de place que le CSV, Parquet 3.0 fois moins.
- **medium** : pandas traite le même nettoyage 847 fois plus vite que Pig (6.8 s contre 5770 s). À ce volume une seule machine suffit : le cluster n'apporte pas de gain de vitesse.
- **ALS, medium** : 2 workers donnent une accélération de ×0.73 (efficacité 37 %). Le gain reste limité : les 2 workers partagent les mêmes CPU et le même disque, et les échanges entre exécuteurs coûtent du temps.
- **Stockage, medium** : ORC occupe 5.1 fois moins de place que le CSV, Parquet 3.5 fois moins.
- **large** : pandas traite le même nettoyage 142 fois plus vite que Pig (42.9 s contre 6082 s). À ce volume une seule machine suffit : le cluster n'apporte pas de gain de vitesse.
- **ALS, large** : 2 workers donnent une accélération de ×0.65 (efficacité 32 %). Le gain reste limité : les 2 workers partagent les mêmes CPU et le même disque, et les échanges entre exécuteurs coûtent du temps.
- **Stockage, large** : ORC occupe 5.1 fois moins de place que le CSV, Parquet 3.6 fois moins.
- **Limite d'une seule machine (extrapolation linéaire)** : pandas utilise 2504 Mo pour 5.0 M de notes ; en extrapolant, 70 % des 8 Go de ce PC seraient atteints vers 12 millions de notes. C'est au-delà de ce seuil, ou quand les données ne tiennent plus sur une machine, que Pig, Hive et Spark deviennent nécessaires. Ordre de grandeur, pas une mesure.

## 7. Limites de ce benchmark

- Le « cluster » est simulé sur un seul PC : les workers partagent CPU, mémoire et disque. Les gains de vitesse mesurés ne sont donc pas ceux d'un vrai cluster de plusieurs machines.
- Données synthétiques, avec environ 1 % de lignes sales ajoutées volontairement.
- Peu de répétitions (3 pour ALS comme pour Hive) : les écarts de quelques pourcents ne sont pas significatifs.
- Les temps Hive de la section 3 incluent le démarrage de la session beeline, retiré par soustraction d'une requête `SELECT 1`.

