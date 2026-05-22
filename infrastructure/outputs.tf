output "dotenv" {
  value = <<-EOT
# Kafka cluster
KAFKA_BOOTSTRAP_SERVERS="${replace(confluent_kafka_cluster.standard.bootstrap_endpoint, "SASL_SSL://", "")}"
# producer
KAFKA_API_KEY="${confluent_api_key.app-producer-kafka-api-key.id}"
KAFKA_API_SECRET="${confluent_api_key.app-producer-kafka-api-key.secret}"
# consumer
KAFKA_CONSUMER_API_KEY="${confluent_api_key.app-consumer-kafka-api-key.id}"
KAFKA_CONSUMER_API_SECRET="${confluent_api_key.app-consumer-kafka-api-key.secret}"

# Schema Registry
SCHEMA_REGISTRY_URL="${data.confluent_schema_registry_cluster.advanced.rest_endpoint}"
SCHEMA_REGISTRY_API_KEY="${confluent_api_key.env-manager-schema-registry-api-key.id}"
SCHEMA_REGISTRY_API_SECRET="${confluent_api_key.env-manager-schema-registry-api-key.secret}"
EOT

  sensitive = true
}
