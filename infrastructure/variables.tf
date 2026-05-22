variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key (also referred as Cloud API ID)"
  type        = string
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}

variable "cc_region" {
  type        = string
  description = "The region where our CC cluster resides"
}

variable "prefix" {
  description = "Prefix for resources"
  type        = string
  default     = "dvogiatzis"
}

variable "cloud_provider" {
  description = "The cloud provider for the Kafka cluster."
  type        = string
  default     = "AWS"
}
