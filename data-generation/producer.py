import logging
import os
import signal
import time

from config import load_kafka_config, load_schema_registry_config
from kafka_producer import create_producer, delivery_callback
from schema_registry import create_schema_registry_client, is_schema_registered, load_schema
from stock.ohlcv import fetch_ohlcv
from tickers import TICKERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

STOCK_TOPIC = os.environ.get("STOCK_TOPIC", "ohlcv_data")
STOCK_SCHEMA_PATH = os.environ.get(
    "STOCK_SCHEMA_PATH", "./schemas/avro/stock_ohlcv.avsc"
)

running = True


def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received")
    running = False


def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    kafka_config = load_kafka_config()
    sr_config = load_schema_registry_config()
    sr_client = create_schema_registry_client(sr_config)

    stock_schema = load_schema(STOCK_SCHEMA_PATH)

    subject = f"{STOCK_TOPIC}-value"
    if is_schema_registered(sr_client, subject):
        logger.info("Schema registered for '%s'", STOCK_TOPIC)
    else:
        logger.info("Schema for '%s' will be auto-registered on first produce", STOCK_TOPIC)

    producer = create_producer(kafka_config, stock_schema, sr_client)

    ohlcv_data = fetch_ohlcv()
    max_days = min(len(records) for records in ohlcv_data.values())
    logger.info("Loaded %d trading days of OHLCV data", max_days)

    logger.info(
        "Producing OHLCV data to '%s'. Ctrl+C to stop.",
        STOCK_TOPIC
    )

    tick = 0

    while running:
        day_index = tick % max_days

        for ticker in TICKERS:
            record = ohlcv_data[ticker][day_index]
            producer.produce(
                topic=STOCK_TOPIC,
                key=ticker,
                value=record,
                on_delivery=delivery_callback,
            )

        producer.poll(0)
        tick += 1
        time.sleep(1)

    logger.info("Flushing remaining messages...")
    producer.flush(timeout=10)
    logger.info("Producer shut down.")


if __name__ == "__main__":
    main()
