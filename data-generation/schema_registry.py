import logging
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient

from config import SchemaRegistryConfig

logger = logging.getLogger(__name__)


def create_schema_registry_client(config: SchemaRegistryConfig) -> SchemaRegistryClient:
    return SchemaRegistryClient(
        {
            "url": config.url,
            "basic.auth.user.info": f"{config.api_key}:{config.api_secret}",
        }
    )


def is_schema_registered(client: SchemaRegistryClient, subject: str) -> bool:
    try:
        version = client.get_latest_version(subject)
        logger.info(
            "Schema '%s' is registered (version=%d, id=%d)",
            subject,
            version.version,
            version.schema_id,
        )
        return True
    except Exception:
        logger.info("Schema '%s' is not yet registered", subject)
        return False


def load_schema(schema_path: str) -> str:
    path = Path(schema_path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path.resolve()}")
    return path.read_text()
