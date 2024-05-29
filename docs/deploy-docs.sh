#!/bin/bash

set -e

docker build --network=host . -f docs/Dockerfile -t docker.chronicle.software:8083/queue-docs-tar && \
docker push docker.chronicle.software:8083/queue-docs-tar
