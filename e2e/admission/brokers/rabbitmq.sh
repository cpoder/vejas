#!/bin/bash
# Real-broker leg for AMQP recipes: a throwaway RabbitMQ container.
#   rabbitmq.sh start <port>   (blocks until the listener answers, prints READY)
#   rabbitmq.sh stop <port>
# Image: rabbitmq:3-alpine (227MB, ~15-25s to ready) — heavier than mosquitto
# but the honest price of certifying against the real thing.
# ⚠ Readiness is a HOST-SIDE TCP probe on purpose: `docker exec
# rabbitmq-diagnostics` during boot runs as root and creates
# /var/lib/rabbitmq/.erlang.cookie root-owned BEFORE the server (user
# rabbitmq) does — the server then cannot read its own cookie and the
# container kills itself. Never exec into a booting RabbitMQ.
set -u
case "$1" in
  start)
    P="$2"
    docker run -d --rm --name "rmq-$P" -p "127.0.0.1:$P:5672" \
      rabbitmq:3-alpine > /dev/null || exit 1
    for _ in $(seq 120); do
      # log-grep readiness: a host TCP probe false-readies against the
      # docker-proxy, and any Erlang CLI exec'd during boot trips the cookie
      # trap above — the broker's own log line is the only clean signal
      docker logs "rmq-$P" 2>&1 | grep -q "started TCP listener" \
        && { sleep 1; echo READY; exit 0; }
      sleep 1
    done
    echo "rabbitmq never came up"; docker rm -f "rmq-$P" > /dev/null 2>&1; exit 1;;
  stop)
    docker rm -f "rmq-$2" > /dev/null 2>&1;;
esac
