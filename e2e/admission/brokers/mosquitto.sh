#!/bin/bash
# Real-broker leg for MQTT recipes: a throwaway mosquitto container.
#   mosquitto.sh start <port>   (blocks until the listener answers, prints READY)
#   mosquitto.sh stop <port>
set -u
case "$1" in
  start)
    P="$2"
    printf 'listener %s\nallow_anonymous true\n' "$P" > "/tmp/mosq-$P.conf"
    docker run -d --rm --name "mosq-$P" -p "127.0.0.1:$P:$P" \
      -v "/tmp/mosq-$P.conf:/mosquitto/config/mosquitto.conf:ro" \
      eclipse-mosquitto > /dev/null || exit 1
    for _ in $(seq 100); do
      (exec 3<>"/dev/tcp/127.0.0.1/$P") 2>/dev/null && { exec 3>&-; echo READY; exit 0; }
      sleep 0.1
    done
    echo "mosquitto never came up"; exit 1;;
  stop)
    docker rm -f "mosq-$2" > /dev/null 2>&1; rm -f "/tmp/mosq-$2.conf";;
esac
