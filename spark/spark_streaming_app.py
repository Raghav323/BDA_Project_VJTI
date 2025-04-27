from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

ssc = StreamingContext(spark.sparkContext, 5)

kafkaStream = KafkaUtils.createStream(
    ssc, "localhost:2181", "spark-streaming-consumer", {"real_time_data": 1}
)

def process_message(message):
    data = json.loads(message[1])
    print(f"Received data: {data}")
    # Insert processing logic here, like writing to HBase

kafkaStream.foreachRDD(lambda rdd: rdd.foreach(process_message))

ssc.start()
ssc.awaitTermination()
