from bytewax.connectors.kafka import operators as kop

import functools
import os
from confluent_kafka import OFFSET_STORED

from broker2db.utils.cache import cached

@cached(ttl = 10_000)
async def get_kafka_input(brokers, config, topic):
    kop_input = functools.partial(kop.input, brokers=brokers, starting_offset=OFFSET_STORED, add_config=config, topics=[topic])
    return kop_input

# KAFKA_USE_SSL = os.getenv('KAFKA_USE_SSL', 'true').lower() == 'true'
# BROKERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
# KAFKA_CONSUMER_CONFIG = {
#         'group.id': 'ml-inference-pipeline',
#         'auto.offset.reset': 'earliest',
#         'enable.auto.commit': True,
# }

# KAFKA_PRODUCER_CONFIG = {

# }

# print(KAFKA_USE_SSL)
# if KAFKA_USE_SSL:
#     KAFKA_SSL_CONFIG = {
#         'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
#         'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
#         'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
#         'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
#         'ssl.ca.location': os.getenv('KAFKA_SSL_CAFILE'),
#         'ssl.certificate.location': os.getenv('KAFKA_SSL_CERTFILE'),
#         'ssl.key.location': os.getenv('KAFKA_SSL_KEYFILE'),
#         'ssl.key.password': os.getenv('KAFKA_SSL_KEY_PASSWORD'),
#     }
#     KAFKA_CONSUMER_CONFIG = {**KAFKA_CONSUMER_CONFIG, **KAFKA_SSL_CONFIG}
#     KAFKA_PRODUCER_CONFIG = {**KAFKA_PRODUCER_CONFIG, **KAFKA_SSL_CONFIG}
#     kop_input = functools.partial(kop.input, brokers=BROKERS, starting_offset=OFFSET_STORED, add_config=KAFKA_CONSUMER_CONFIG)
#     # kop_output = functools.partial(kop.output, brokers=BROKERS, add_config=KAFKA_SSL_CONFIG)
#     kop_output = functools.partial(kop.output, brokers=BROKERS, add_config=KAFKA_PRODUCER_CONFIG)

# else:
#     kop_input = functools.partial(kop.input, brokers=BROKERS, starting_offset=OFFSET_STORED, add_config=KAFKA_CONSUMER_CONFIG)
#     kop_output = functools.partial(kop.output, brokers=BROKERS, add_config=KAFKA_PRODUCER_CONFIG)




