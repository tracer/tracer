#!/usr/bin/env bash

set -x

PROJECT_NAME='github.com/tracer/tracer'
PROJECT_DIR="${PWD}"

CONTAINER_GOPATH='/go'
CONTAINER_PROJECT_DIR="${CONTAINER_GOPATH}/src/${PROJECT_NAME}"
CONTAINER_PROJECT_GOPATH="${CONTAINER_GOPATH}"

docker run --rm \
    --net="host" \
    -v ${PROJECT_DIR}:${CONTAINER_PROJECT_DIR} \
    -e CI=true \
    -e GODEBUG=netdns=go \
    -e GOPATH=${CONTAINER_PROJECT_GOPATH} \
    -w "${CONTAINER_PROJECT_DIR}" \
    golang:1.7rc1-alpine \
    go get ${PROJECT_NAME} && go test -v -race ./... 2> output.log

EXIT_CODE=$?

cat output.log

if [ ${EXIT_CODE} != 0 ]; then
    exit ${EXIT_CODE}
fi

# Check for race conditions as we don't have a proper exit code for them from the tool
cat output.log | grep -v 'WARNING: DATA RACE'

EXIT_CODE=$?

# If we don't find a match then we don't have a race condition
if [ ${EXIT_CODE} == 1 ]; then
    rm -f output.log
    exit 0
fi

exit ${EXIT_CODE}
