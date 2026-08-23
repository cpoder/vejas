#!/bin/bash
exec "$(dirname "$0")/../../../../e2e/admission/brokers/mosquitto.sh" "$@"
