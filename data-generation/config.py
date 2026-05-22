import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    api_key: str
    api_secret: str


@dataclass(frozen=True)
class SchemaRegistryConfig:
    url: str
    api_key: str
    api_secret: str


def load_kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        api_key=os.environ["KAFKA_API_KEY"],
        api_secret=os.environ["KAFKA_API_SECRET"],
    )


def load_schema_registry_config() -> SchemaRegistryConfig:
    return SchemaRegistryConfig(
        url=os.environ["SCHEMA_REGISTRY_URL"],
        api_key=os.environ["SCHEMA_REGISTRY_API_KEY"],
        api_secret=os.environ["SCHEMA_REGISTRY_API_SECRET"],
    )
