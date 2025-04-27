raw_data = LOAD '/user/hive/warehouse/events' USING PigStorage(',') AS (user_id:int, timestamp:long, event_type:chararray);

grouped_data = GROUP raw_data BY event_type;

event_counts = FOREACH grouped_data GENERATE group AS event_type, COUNT(raw_data) AS event_count;

STORE event_counts INTO '/user/output/event_counts' USING PigStorage(',');
