#!/bin/bash
export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
kafka-server-start.sh -daemon /opt/kafka/default/config/server.properties