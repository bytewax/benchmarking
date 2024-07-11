# Comparing PyFlink and Bytewax Performance

PyFlink requires you to use Python 3.8 for some of the dependencies to work. It is advised to have a separate python environment. One for PyFlink using version 3.8 and another for Bytewax using >3.8.

## Methodology

For this test, what we are going to do is look at the average throughput for a workload in PyFlink and Bytewax that is doing the same thing. The workload will consume from Kafka, create a window and sum the values in that window and write those windows out to another Kafka topic.

To do this, we will generate synthetic data to a kafka topic. 10,000,000 records to a single partition of the format:
```
    {
        id: ...,
        time: ...,
        value: ...
    }
```
Where the time is the event time that the value was produced. This is the data we will use in our stream processing workloads.

The next step will consume from that topic, do the event window computations, give it a system timestamp and produce it to a new kafka topic. The new topic will be a record of how fast we are able to consume all of the events when we look at the first timestamp and the last timestamp.

## PyFlink Install Dependencies

create a new environment with python 3.8
install `requirements.txt` from the flink sub-directory

## Bytewax Install Dependencies

Create a new environment with Python 3.11
install `requirements.txt` in the bytewax sub-directory

## Generate data

You will need a locally available kafka or redpanda cluster. To do so, you can run docker compose.

```console
docker compose up -d
```

Create the topics for input and output

```console
docker exec -it redpanda-0 rpk topic create bench_in
docker exec -it redpanda-0 rpk topic create bench_out_bytewax
docker exec -it redpanda-0 rpk topic create bench_out_flink
```

Generate data if by running the included script:

```console
python generate_data.py --iterations 1000000 --sensors 10 --batch_size 1 --topic bench_in --bootstrap_servers localhost:19092
```

## Running the Flink Script

```
python flink_local.py
```

## Running the Bytewax script

```console
python -m bytewax.run "iot.py:main()"
```

Simple version of throughput calc.

Using the redpanda console at http://localhost:8080 you can view the output topics for both bytewax and flink. You can take the earliest and latest timestamp and the difference is the time to compute the 10 Million events generated in the generate data script.
