"""
Kafka Helper Functions
"""
import functools

from bytewax.connectors.kafka import operators as kop
from confluent_kafka import OFFSET_STORED

from broker2db.utils.cache import cached

@cached(ttl = 10_000)
async def get_kafka_input(brokers, config, topic):
    """
    Get Kafka Input Operator for the given config
    """
    kop_input = functools.partial(kop.input,
                                  brokers=brokers,
                                  starting_offset=OFFSET_STORED,
                                  add_config=config,
                                  topics=[topic])
    return kop_input
