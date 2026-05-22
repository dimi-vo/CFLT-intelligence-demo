import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from config import KafkaConfig

logger = logging.getLogger(__name__)


def delivery_callback(err, msg):
    if err:
        logger.error("Delivery failed: %s", err)
    else:
        logger.info(
            "Delivered to %s [%d] @ %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def create_producer(
    kafka_config: KafkaConfig,
    schema_str: str,
    sr_client: SchemaRegistryClient,
) -> SerializingProducer:
    avro_serializer = AvroSerializer(sr_client, schema_str)

    return SerializingProducer(
        {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": kafka_config.api_key,
            "sasl.password": kafka_config.api_secret,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )
