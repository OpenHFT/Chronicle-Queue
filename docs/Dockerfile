#
# Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
#

FROM node:alpine AS base
RUN apk --update add git
RUN npm install -g gitlab:antora/xref-validator

FROM base AS docs
COPY . /repo
WORKDIR /repo/docs/antora
RUN npm ci && npm run build-portal
RUN find . -type f -print0 | xargs -0 sed -i /prism\.js/d
ENTRYPOINT tar -C/repo/docs/antora/build/site -cf- .
