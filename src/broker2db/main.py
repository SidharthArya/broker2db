"""
Main Entrypoint
"""
from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.kafka import KafkaSourceMessage
# from broker2db.utils.arguments import args
from bytewax.dataflow import Dataflow
from sqlalchemy import text

import asyncio
from broker2db.broker.kafka import get_kafka_input
from broker2db.db.postgres import get_engine
import tomllib
import orjson

with open("examples/example.toml", "rb") as f:
    t = tomllib.load(f)

# Parse all arguments and settings
# print(t, args)
if "settings" in t:
    settings = t["settings"]
else:
    settings = {}

def serialize_source(x):
    values = orjson.loads(x.value)
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
    columns = []
    values = []
    for k, v in x.value.items():
        columns.append(k)
        if type(v) != str:
            values.append(str(v))
        else:
            values.append(f"'{v}'")
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    return query

def execute_query(query, engine):
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()
    return "Success"

async def main(flow):
    for k, v in t.items():
        if k == "settings":
            continue
        print(k, v)
        if v["source-type"] == 'kafka':
            print("Here")
            brokers = [settings["kafka-broker"]]
            source_input = (await get_kafka_input(brokers=brokers, config={}, topic=v["source"]))
            source = source_input(f"{k}-source", flow)
            source_deser = op.map("{k}-source-deser", source.oks, serialize_source)
        
        if v["destination-type"] == 'postgres':
            target_engine = get_engine(settings['postgres-url'], v['postgres-database'])
            destination = v['destination']
            queries = op.map(f"{k}-query", source_deser, lambda x: make_query(x, destination))
            push = op.map(f"{k}-to-destination", queries, lambda query: execute_query(query, target_engine))
            op.inspect("inspector", queries)

print(__name__)
# DataFlow
flow = Dataflow("broker2db")
asyncio.run(main(flow))