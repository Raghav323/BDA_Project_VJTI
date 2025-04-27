CREATE EXTERNAL TABLE IF NOT EXISTS events (
    user_id INT,
    timestamp BIGINT,
    event_type STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:timestamp,info:event_type")
TBLPROPERTIES ("hbase.table.name" = "events_table");

SELECT event_type, COUNT(*) FROM events GROUP BY event_type;
