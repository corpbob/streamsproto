KAFKA_ROUTE=$1
TOPIC=$2

kafka-console-producer.sh --broker-list $KAFKA_ROUTE:443 --topic $TOPIC
