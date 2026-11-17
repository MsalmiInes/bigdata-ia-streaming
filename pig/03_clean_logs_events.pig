-- ============================================================
-- 03_clean_logs_events.pig : watch_logs et kafka_events
--   watch_logs   : duree entre 1 et 600 min, device connu, doublons supprimes
--   kafka_events : event_type parmi play / pause / like / search
--   (pas de DISTINCT sur les evenements : deux clics identiques peuvent etre legitimes)
-- ============================================================
SET job.name 'pig-03-clean-logs-events';
SET default_parallel 2;

-- ---------------------------------------------------------- WATCH_LOGS
lg_raw = LOAD '/streaming/raw/watch_logs/watch_logs.csv' USING PigStorage(',')
         AS (log_id:chararray, user_id:chararray, movie_id:chararray,
             watch_duration_min:chararray, device:chararray, ts:chararray);
lg_body = FILTER lg_raw BY log_id != 'log_id';
lg_typed = FOREACH lg_body GENERATE
           (long)log_id AS log_id,
           (int)user_id AS user_id,
           (int)movie_id AS movie_id,
           (int)watch_duration_min AS watch_duration_min,
           LOWER(TRIM(device)) AS device,
           ts;
SPLIT lg_typed INTO
      lg_ok  IF (log_id IS NOT NULL AND user_id IS NOT NULL AND movie_id IS NOT NULL
                 AND watch_duration_min IS NOT NULL AND watch_duration_min >= 1
                 AND watch_duration_min <= 600 AND device matches 'mobile|tv|web|tablet'),
      lg_bad OTHERWISE;
lg_clean = DISTINCT lg_ok;
lg_rej = FOREACH lg_bad GENERATE log_id, user_id, movie_id, watch_duration_min, device, ts,
         'duree_ou_appareil_invalide' AS reason;
fs -rm -r -f /streaming/clean/watch_logs;
fs -rm -r -f /streaming/rejects/watch_logs;
STORE lg_clean INTO '/streaming/clean/watch_logs' USING PigStorage(',');
STORE lg_rej   INTO '/streaming/rejects/watch_logs' USING PigStorage(',');

-- ---------------------------------------------------------- KAFKA_EVENTS
ev_raw = LOAD '/streaming/raw/kafka_events/kafka_events.csv' USING PigStorage(',')
         AS (user_id:chararray, movie_id:chararray, event_type:chararray, ts:chararray);
ev_body = FILTER ev_raw BY user_id != 'user_id';
ev_typed = FOREACH ev_body GENERATE (int)user_id AS user_id, (int)movie_id AS movie_id,
           LOWER(TRIM(event_type)) AS event_type, ts;
SPLIT ev_typed INTO
      ev_ok  IF (user_id IS NOT NULL AND movie_id IS NOT NULL AND ts IS NOT NULL
                 AND event_type matches 'play|pause|like|search'),
      ev_bad OTHERWISE;
ev_rej = FOREACH ev_bad GENERATE user_id, movie_id, event_type, ts, 'evenement_invalide' AS reason;
fs -rm -r -f /streaming/clean/kafka_events;
fs -rm -r -f /streaming/rejects/kafka_events;
STORE ev_ok  INTO '/streaming/clean/kafka_events' USING PigStorage(',');
STORE ev_rej INTO '/streaming/rejects/kafka_events' USING PigStorage(',');
