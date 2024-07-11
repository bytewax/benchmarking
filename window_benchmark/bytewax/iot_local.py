"""Example of stateful stream processing with Bytewax."""

from datetime import timedelta, datetime, timezone
import orjson as json
import time

from bytewax.dataflow import Dataflow
import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.operators.windowing import EventClock, TumblingWindower
from bytewax.connectors.kafka import KafkaSourceMessage, KafkaSinkMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.stdio import StdOutSink

from bytewax.testing import run_main
from typing import Tuple


def deserialize(msg: KafkaSourceMessage) -> Tuple[str, dict]:
    """Deserialize Kafka messages.

    Will return tuples of (id, msg).
    """
    payload = json.loads(msg.value.decode("utf-8"))["value"]
    return payload


def get_event_time(event):
    """Extract event-time from data."""
    # Remember timezone info!
    return datetime.fromisoformat(event["time"])

def main(addr: str = "localhost:19092", topic_in: str = "bench_in", topic_out: str = "bench_out_bytewax", wint: int = 10):
    """Run the stream processing flow."""
    # We want to do windowing based on event times!
    clock = EventClock(get_event_time, timedelta(seconds=1))

    align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
    # We'll operate in 10 second windows
    window = TumblingWindower(align_to=align_to, length=timedelta(seconds=wint))

    # Initialize a flow
    flow = Dataflow("bench_bytewax")
    # Input is our Kafka stream
    kinp = kop.input(
        "input", flow, brokers=[addr], topics=[topic_in], tail=False)
    errs = op.inspect("errors", kinp.errs).then(op.raises, "crash-on-err")

    # Extract dictionaries from JSON messages
    stream = op.map("deser", kinp.oks, deserialize)
    # Extract (key, value) pairs with the data we want to operate on as the
    # value
    keyed_stream = op.key_on("key", stream, lambda x: str(x['id']))

    def add(acc, x):
        acc["value"] += x["value"]
        return acc
    
    # reduce each key according to our reducer function, bytewax will pass only
    # the values to the reduce function. Since we output dicts from the
    # previous map, we need to output dicts from the reducer
    windowed_stream = win.reduce_window(
            "sum", keyed_stream, clock, window, add
    )

    def serializer(key__metadata_reduced):
        key, (metadata, reduced) = key__metadata_reduced
        formatted = {"id":key, "time": time.time(), "value":reduced["value"]}
        return KafkaSinkMessage(None, json.dumps(formatted))

    serialized = op.map("serde", windowed_stream.down, serializer)
    
    # make topic bench_out_bytewax before starting
    kop.output('out', serialized, brokers=[addr], topic=topic_out)

    run_main(flow)



if __name__ == "__main__":
    main()