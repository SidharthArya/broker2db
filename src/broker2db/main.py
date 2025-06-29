"""
Main Entrypoint
"""
import asyncio
import tomllib
import functools

from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSourceMessage
# from broker2db.utils.arguments import args
from bytewax.dataflow import Dataflow
from sqlalchemy import text
import orjson

from broker2db.broker.kafka import get_kafka_input
from broker2db.db.postgres import get_engine

with open("examples/example.toml", "rb") as f:
    t = tomllib.load(f)

# Parse all arguments and settings
# print(t, args)
if "settings" in t:
    settings = t["settings"]
else:
    settings = {}

def serialize_source(x):
    """
    Serialize Kafka Source
    """
    values = orjson.loads(x.value)  # pylint: disable=no-member
    headers = x.headers
    latency = x.latency
    offset = x.offset
    partition = x.partition
    timestamp = x.timestamp
    key = x.key
    return KafkaSourceMessage(
        value = values,
        headers = headers,
        latency = latency,
        offset = offset,
        partition = partition,
        timestamp = timestamp,
        key = key
    )

def make_query(x, table_name):
    """
    Make Postgres Query
    """
    columns = []
    values = []
    for k, v in x.value.items():
        columns.append(k)
        if isinstance(v, str):
            values.append(str(v))
        else:
            values.append(f"'{v}'")
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    return query

def execute_query(query, engine):
    """
    Executes Insert or Update Query in Postgres
    """
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()
    return "Success"

async def main(flow):
    """
    Main Flow for broker2db
    """
    for k, v in t.items():
        if k == "settings":
            continue
        print(k, v)
        if v["source-type"] == 'kafka':
            print("Here")
            brokers = [settings["kafka-broker"]]
            source_input = await get_kafka_input(brokers=brokers, config={}, topic=v["source"])
            source = source_input(f"{k}-source", flow)
            source_deser = op.map("{k}-source-deser", source.oks, serialize_source)
        else:
            continue

        if v["destination-type"] == 'postgres':
            query_function = functools.partial(lambda x, v: make_query(x, v["destination"]), v=v)
            queries = op.map(f"{k}-query", source_deser, query_function)
            engine = get_engine(settings['postgres-url'], v['postgres-database'])
            query_exec = functools.partial(execute_query, engine=engine)
            push = op.map(f"{k}-to-destination", queries, query_exec)
            op.inspect("inspector", push)

print(__name__)
# DataFlow
dataflow = Dataflow("broker2db")
asyncio.run(main(dataflow))
