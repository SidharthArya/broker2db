"""
Process all settings
 Precedence
 1. direct arguments
 2. Toml Config
 3. Yaml Config
 4. Json Config
 5. Environment Variables

"""
import argparse
from argparse import Action
import os

parser = argparse.ArgumentParser(
    prog="broker2db",
    description="Easily push all your broker data to database"
)
# Kafka Arguments
parser.add_argument("-sk", "--source-kafka", action="store_true", help="Use Kafka as source")
parser.add_argument("-skh", "--source-kafka-host", help="Kafka Bootstrap Server")
args = parser.parse_args()