#!/bin/bash
set -eu

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS not set in .env}"
: "${SECRETS_DIR:?SECRETS_DIR not set in .env}"

acl_topic() {
  /opt/kafka/bin/kafka-acls.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
    --command-config "${SECRETS_DIR}/client.properties" \
    --add --allow-principal "User:$1" \
    --operation "$2" \
    --topic "$3"
}

acl_group() {
  /opt/kafka/bin/kafka-acls.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
    --command-config "${SECRETS_DIR}/client.properties" \
    --add --allow-principal "User:$1" \
    --operation "$2" \
    --group "$3"
}

acl_cluster() {
  /opt/kafka/bin/kafka-acls.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
    --command-config "${SECRETS_DIR}/client.properties" \
    --add --allow-principal "User:$1" \
    --operation "$2" \
    --cluster
}

# order-api
## create + produce to orders.placed
acl_topic "CN=order-api" Create orders.placed
acl_topic "CN=order-api" Write orders.placed
## consume from orders.[accepted|rejected]
acl_topic "CN=order-api" Read orders.accepted
acl_topic "CN=order-api" Read orders.rejected
## consumer group order-api
acl_group "CN=order-api" Read order-api

# inventory-service
## consume from orders.placed
acl_topic "CN=inventory-service" Read orders.placed
## consumer group inventory-service
acl_group "CN=inventory-service" Read inventory-service

# kafka-ui
## read-only everywhere
acl_cluster "CN=kafka-ui" Describe
acl_cluster "CN=kafka-ui" DescribeConfigs
acl_topic "CN=kafka-ui" Read '*'
acl_group "CN=kafka-ui" Describe '*'
acl_topic "CN=kafka-ui" DescribeConfigs '*'

# schema-registry
## everything on _schemas
acl_topic "CN=schema-registry" All _schemas
## consumer group schema-registry
acl_group "CN=schema-registry" Read schema-registry

# connect/debezium
## everything on debezium.*
acl_topic "CN=connect" All debezium.configs
acl_topic "CN=connect" All debezium.offsets
acl_topic "CN=connect" All debezium.statuses
## create + produce to orders.[accepted|rejected]
acl_topic "CN=connect" Create orders.accepted
acl_topic "CN=connect" Write orders.accepted
acl_topic "CN=connect" Create orders.rejected
acl_topic "CN=connect" Write orders.rejected
## consumer group debezium
acl_group "CN=connect" Read debezium
