#!/bin/bash
#
# Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
#


set -e

docker build --network=host . -f docs/Dockerfile -t docker.chronicle.software:8083/queue-docs-tar && \
docker push docker.chronicle.software:8083/queue-docs-tar
