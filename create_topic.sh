#BOOTSTRAP_SERVER=localhost:9092
BOOTSTRAP_SERVER=$1
TOPIC=$2
kafka-topics.sh --create \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --replication-factor 1 \
    --partitions 2 \
    --topic $TOPIC
