import json
from datetime import datetime, timezone
import random
import argparse
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def create_iot_events(iters=10, n=5, batch_size=5, sl=0, topic_name="bench_in", bootstrap_servers='localhost:9092'):
    # Configure the producer
    conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(**conf)

    for i in range(iters):
        for item in range(batch_size):   
            readings = [
                {
                    "value": {
                        "id": sens_id,
                        "value": random.uniform(5,35),
                        "time": datetime.now(timezone.utc).isoformat(),
                    },
                }
                for sens_id in range(n)
            ]
            for reading in readings:
                data = json.dumps(reading)
                print(data)  # For debugging
                # Produce the message
                producer.produce(topic_name, value=data.encode('utf-8'), callback=delivery_report)
            producer.flush()  # Ensure all messages are sent

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run loop with specified options.')
    parser.add_argument('--iterations', type=int, default=10, help='Number of iterations for the loop')
    parser.add_argument('--sensors', type=int, default=5, help='Number of sensors to use')
    parser.add_argument('--batch_size', type=int, default=5, help='Number of records per batch')
    parser.add_argument('--sleep', type=int, default=0, help='Seconds to pause between batches')
    parser.add_argument('--topic', type=str, default="bytewax", help='Kafka topic to write to')
    parser.add_argument('--bootstrap_servers', type=str, default="localhost:9092", help='Kafka cluster bootstrap servers')

    args = parser.parse_args()

    print(f"Running {args.iterations} iterations with {args.sensors} sensors.")
    create_iot_events(iters=args.iterations, 
                      n=args.sensors,
                      batch_size=args.batch_size,
                      sl=args.sleep, 
                      topic_name=args.topic, 
                      bootstrap_servers=args.bootstrap_servers)
