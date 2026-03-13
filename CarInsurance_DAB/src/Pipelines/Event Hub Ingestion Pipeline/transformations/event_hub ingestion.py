from pyspark import pipelines as dp
from pyspark.sql.types import *
from pyspark.sql.functions import *


EH_NAMESPACE = spark.conf.get("eh.namespace")
EH_NAME = spark.conf.get("eh.name")
EH_CONN_STR = spark.conf.get("eh.connectionString")

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "maxOffsetsPerTrigger": "50000",
    "failOnDataLoss": "true",
    "startingOffsets": "earliest",
}

payload_schema = StructType([
    StructField("chassis_no", StringType(), True),
    StructField("latitude", DoubleType(), True),   # You can use DoubleType for coordinates!
    StructField("longitude", DoubleType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("speed", FloatType(), True)      # You can use Integer/Double for speed
])

@dp.table(name="car_telematics", table_properties={"quality": "bronze"})
def telematics():
    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    parsed_stream = raw_stream.selectExpr(
        "CAST(value AS STRING) AS raw_json",       # 'value' contains your payload
        "CAST(key AS STRING) AS partitionKey",     # 'key' is the partition key
        "topic AS stream",                         # 'topic' is the Event Hub name
        "partition AS shardId",                    # 'partition' is the shard ID
        "offset AS sequenceNumber",                # <-- HERE IT IS: 'offset' becomes sequenceNumber
        "timestamp AS approximateArrivalTimestamp" # 'timestamp' is the arrival time
    ).withColumn(
        "decoded_data",
        from_json(col("raw_json"), payload_schema)
    ).withColumn(
        "stream_metadata",
        struct(
            col("partitionKey"),
            col("stream"),
            col("shardId"),
            col("sequenceNumber"),
            col("approximateArrivalTimestamp")
        )
    )
    
    return parsed_stream.select(
        col("decoded_data.chassis_no"),
        col("decoded_data.latitude"),
        col("decoded_data.longitude"),
        col("decoded_data.event_timestamp"),
        col("decoded_data.speed"),
        col("stream_metadata")
    )