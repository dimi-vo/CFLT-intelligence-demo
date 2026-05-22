resource "random_id" "resource_suffix" {
  byte_length = 4
}
# Create new environment
resource "confluent_environment" "staging" {
  display_name = "${var.prefix}-demo"

  stream_governance {
    package = "ADVANCED"
  }
}

# Get reference of Schema Registry Cluster
data "confluent_schema_registry_cluster" "advanced" {
  environment {
    id = confluent_environment.staging.id
  }

  depends_on = [
    confluent_kafka_cluster.standard
  ]
}

# Create standard Cluster
resource "confluent_kafka_cluster" "standard" {
  display_name = "${var.prefix}-cluster"
  availability = "SINGLE_ZONE"
  cloud        = var.cloud_provider
  region       = var.cc_region
  standard {}
  environment {
    id = confluent_environment.staging.id
  }
}

# Create an SA to manage the cluster
resource "confluent_service_account" "app-manager" {
  display_name = "${var.prefix}-app-manager"
  description  = "Service account to manage Kafka cluster"
}

# Role Binding for interacting with the Kafka cluster
resource "confluent_role_binding" "app-manager-kafka-cluster-admin" {
  principal   = "User:${confluent_service_account.app-manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.standard.rbac_crn
}

# Confluent API Key for the app-manager SA
resource "confluent_api_key" "app-manager-kafka-api-key" {
  display_name = "${var.prefix}-app-manager-kafka-api-key"
  description  = "Kafka API Key that is owned by 'app-manager' service account"
  owner {
    id          = confluent_service_account.app-manager.id
    api_version = confluent_service_account.app-manager.api_version
    kind        = confluent_service_account.app-manager.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.standard.id
    api_version = confluent_kafka_cluster.standard.api_version
    kind        = confluent_kafka_cluster.standard.kind

    environment {
      id = confluent_environment.staging.id
    }
  }

  depends_on = [
    confluent_role_binding.app-manager-kafka-cluster-admin
  ]
}

# Create the stock_data topic
resource "confluent_kafka_topic" "stock_data" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  topic_name    = "ohlcv_data"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

# Create the news topic
resource "confluent_kafka_topic" "news" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  topic_name    = "news"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

# Create the news topic
resource "confluent_kafka_topic" "enriched_news" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  topic_name    = "enriched_news"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_topic" "generated_data" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  topic_name    = "purchases"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_service_account" "app-consumer" {
  display_name = "${var.prefix}-app-consumer"
  description  = "Service account to consume from Kafka topics"
}

resource "confluent_api_key" "app-consumer-kafka-api-key" {
  display_name = "${var.prefix}-app-consumer-kafka-api-key"
  description  = "Kafka API Key that is owned by 'app-consumer' service account"
  owner {
    id          = confluent_service_account.app-consumer.id
    api_version = confluent_service_account.app-consumer.api_version
    kind        = confluent_service_account.app-consumer.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.standard.id
    api_version = confluent_kafka_cluster.standard.api_version
    kind        = confluent_kafka_cluster.standard.kind

    environment {
      id = confluent_environment.staging.id
    }
  }
}

resource "confluent_kafka_acl" "app-consumer-all-topics" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-consumer.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-consumer-all-groups" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "GROUP"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-consumer.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}


# SA for the producer
resource "confluent_service_account" "app-producer" {
  display_name = "${var.prefix}-app-producer"
  description  = "Service account to produce to Kafka topics"
}

# API Key for the producer SA
resource "confluent_api_key" "app-producer-kafka-api-key" {
  display_name = "${var.prefix}-app-producer-kafka-api-key"
  description  = "Kafka API Key that is owned by 'app-producer' service account"
  owner {
    id          = confluent_service_account.app-producer.id
    api_version = confluent_service_account.app-producer.api_version
    kind        = confluent_service_account.app-producer.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.standard.id
    api_version = confluent_kafka_cluster.standard.api_version
    kind        = confluent_kafka_cluster.standard.kind

    environment {
      id = confluent_environment.staging.id
    }
  }
}

# ACL for the producer to write to the stock_data topic
resource "confluent_kafka_acl" "app-producer-write-on-stock-data-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.stock_data.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-producer.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

# ACL for the producer to write to the news topic
resource "confluent_kafka_acl" "app-producer-write-on-news-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.news.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-producer.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

# ACL for the producer to write to the news topic
resource "confluent_kafka_acl" "app-producer-write-on-enriched-news-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.enriched_news.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-producer.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

# SA for managing the environment. Needed to create the schema
resource "confluent_service_account" "env-manager" {
  display_name = "${var.prefix}-env-manager"
  description  = "Service account to manage 'Staging' environment"
}

# Grant Environment Admin role to the SA
resource "confluent_role_binding" "env-manager-environment-admin" {
  principal   = "User:${confluent_service_account.env-manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.staging.resource_name
}

# Create API Key for the env-manager SA
resource "confluent_api_key" "env-manager-schema-registry-api-key" {
  display_name = "${var.prefix}-env-manager-schema-registry-api-key"
  description  = "Schema Registry API Key that is owned by 'env-manager' service account"
  owner {
    id          = confluent_service_account.env-manager.id
    api_version = confluent_service_account.env-manager.api_version
    kind        = confluent_service_account.env-manager.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.advanced.id
    api_version = data.confluent_schema_registry_cluster.advanced.api_version
    kind        = data.confluent_schema_registry_cluster.advanced.kind

    environment {
      id = confluent_environment.staging.id
    }
  }

  depends_on = [
    confluent_role_binding.env-manager-environment-admin
  ]
}


# Create the service account for the DataGen Connector
resource "confluent_service_account" "app-connector" {
  display_name = "${var.prefix}-SA-datagen-connector"
}

resource "confluent_kafka_acl" "app-connector-describe-on-cluster" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "CLUSTER"
  resource_name = "kafka-cluster"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-connector.id}"
  host          = "*"
  operation     = "DESCRIBE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-connector-write-on-target-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.generated_data.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-connector.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_connector" "source" {
  environment {
    id = confluent_environment.staging.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }

  // Block for custom *sensitive* configuration properties that are labelled with "Type: password" under "Configuration Properties" section in the docs:
  // https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html#configuration-properties
  config_sensitive = {}

  // Block for custom *nonsensitive* configuration properties that are *not* labelled with "Type: password" under "Configuration Properties" section in the docs:
  // https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html#configuration-properties
  config_nonsensitive = {
    "connector.class"          = "DatagenSource"
    "name"                     = "SmoothDataGenerator"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app-connector.id
    "kafka.topic"              = confluent_kafka_topic.generated_data.topic_name
    "output.data.format"       = "AVRO"
    "quickstart"               = "PURCHASES"
    "tasks.max"                = "1"
  }

  depends_on = [
    confluent_kafka_acl.app-connector-describe-on-cluster,
    confluent_kafka_acl.app-connector-write-on-target-topic
  ]
}

#Keep this
resource "confluent_flink_compute_pool" "flinkpool-main" {
  display_name = "${var.prefix}_standard_compute_pool_${random_id.resource_suffix.hex}"
  cloud        = var.cloud_provider
  region       = var.cc_region
  max_cfu      = 10
  environment {
    id = confluent_environment.staging.id
  }
}
