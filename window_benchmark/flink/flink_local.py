"""Example of stateful streaming with Flink."""

import json
from pathlib import Path

from datetime import datetime
import time

from pyflink.common import Time, WatermarkStrategy, SimpleStringSchema, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
)
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types


class SensorTimestampAssigner(TimestampAssigner):
    """Custom event-time extraction."""

    def extract_timestamp(self, value, record_timestamp) -> int:
        """Get event time in milliseconds"""
        return int(datetime.fromisoformat(value["time"]).timestamp() * 1000)

def main(
    addr: str = "localhost:19092",
    topic_in: str = "bench_in",
    topic_out: str = "bench_out_flink",
    win: int = 10,
):
    """Analyze the Kafka stream."""
    env = StreamExecutionEnvironment.get_execution_environment()
    cwd = Path.cwd()
    # env.add_jars(
    #     "file:///opt/flink/flink-connector-kafka-3.1.0-1.18.jar",
    #     "file:///opt/flink/kafka-clients-3.7.0.jar",
    # )
    env.add_jars(
        "file://{cwd}/flink-connector-kafka-3.1.0-1.18.jar",
        "file://{cwd}/kafka-clients-3.7.0.jar",
    )

    env.set_parallelism(1)

    # Kafka source will run in unbounded mode by default
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(addr)
        .set_topics(topic_in)
        .set_group_id("flink-bench")
        .set_value_only_deserializer(SimpleStringSchema(charset="utf-8"))
        .build()
    )

    # Set up Kafka Producer
    record_serializer = KafkaRecordSerializationSchema.builder() \
    .set_topic(topic_out) \
    .set_value_serialization_schema(SimpleStringSchema(charset="utf-8")) \
    .build()

    kafka_sink = (
        KafkaSink.builder() \
        .set_bootstrap_servers(addr) \
        .set_record_serializer(record_serializer) \
        .build()
        )

    (
        # # Ingest from our Kafka source
        env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            "Kafka Source",
        )
        ## For local test, uncomment below
        # env.from_collection(
        # collection=[json.dumps({"id":1, "value":10, "time":"2024-06-25T11:33:36.000000+00:00"}),
        #             json.dumps({"id":2, "value":12, "time":"2024-06-25T11:33:36.000000+00:00"}), 
        #             json.dumps({"id":1, "value":10, "time":"2024-06-25T11:33:40.000000+00:00"}),
        #             json.dumps({"id":2, "value":12, "time":"2024-06-25T11:33:40.000000+00:00"}),
        #             json.dumps({"id":1, "value":10, "time":"2024-06-25T11:33:44.000000+00:00"}),
        #             json.dumps({"id":2, "value":12, "time":"2024-06-25T11:33:44.000000+00:00"}),
        #             json.dumps({"id":1, "value":10, "time":"2024-06-25T11:33:50.000000+00:00"}),
        #             json.dumps({"id":2, "value":12, "time":"2024-06-25T11:33:50.000000+00:00"})])
        # Read the json-strings into dictionaries
        .map(lambda x: json.loads(x)["value"])
        # Assign watermarks here, since we can access the event time from our
        # json-data. Watermarks are needed for event-time windowing below.
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1))
            # Don't wait too long for idle data sources
            .with_idleness(Duration.of_millis(500))
            .with_timestamp_assigner(SensorTimestampAssigner())
        )
        # Map each dictionary into (key, value) tuples
        .map(lambda x: (x["id"], x["value"]))
        # Group by unique keys
        .key_by(lambda x: x[0])
        # ...and collect each key into their Tumbling windows
        .window(TumblingEventTimeWindows.of(Time.seconds(win)))
        # ...which are reduced to the sum of value-fields inside each window
        .reduce(lambda x, y: (x[0], x[1] + y[1]))
        .map(lambda x: {"id":x[0], "time": time.time(), "value":x[1]})
        .map(lambda record: json.dumps(record), Types.STRING())
        .sink_to(kafka_sink)
        # .print()
    )

    # submit for execution
    env.execute()


if __name__ == "__main__":
    main(
        addr = "localhost:19092",
        topic_in = "bench_in",
        topic_out = "bench_out_flink",
        win = 10)
