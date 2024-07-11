from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow

BROKERS = ["localhost:19092"]
IN_TOPICS = ["in_topic"]
OUT_TOPIC = "out_topic"

flow = Dataflow("kafka_in_out")
kinp = kop.input("inp", flow, brokers=BROKERS, topics=IN_TOPICS)
op.inspect("inspect-errors", kinp.errs)
op.inspect("inspect-oks", kinp.oks)

def drop_one_percent(count, item):
    """Filter function to drop approximately one percent of the data."""
    if not count:
        count = 0
    count += 1
    if count % 100 == 0:
        return (count, False)
    else:
        return (count, True)
    
filtered = op.stateful_map("count", drop_one_percent, initial_state_factory=dict).then(op.filter, "filter", lambda x: x)
op.inspect("filtered")
# kop.output("out1", kinp.oks, brokers=BROKERS, topic=OUT_TOPIC)