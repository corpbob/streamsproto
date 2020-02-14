#BOOTSTRAP_SERVER=localhost:9092
BOOTSTRAP_SERVER=$1
kafka-topics.sh --list --bootstrap-server $BOOTSTRAP_SERVER
