#!/usr/bin/env bash

set -x

CONTAINER_NAME=${1}
CONTAINER_TAG=${2}

PROJECT_NAME='github.com/tracer/tracer'
PROJECT_DIR="${PWD}"

CONTAINER_GOPATH='/go'
CONTAINER_PROJECT_DIR="${CONTAINER_GOPATH}/src/${PROJECT_NAME}"
CONTAINER_PROJECT_GOPATH="${CONTAINER_GOPATH}"

docker run --rm \
        -v ${PROJECT_DIR}:${CONTAINER_PROJECT_DIR} \
        -e GOPATH=${CONTAINER_PROJECT_GOPATH} \
        -e CGO_ENABLED=0 \
        -e GODEBUG=netdns=go \
        -w "${CONTAINER_PROJECT_DIR}" \
        golang:1.7rc1-alpine \
        go build -v -o tracer ${PROJECT_NAME}/cmd/tracer

docker run --rm \
        -v ${PROJECT_DIR}:${CONTAINER_PROJECT_DIR} \
        -e GOPATH=${CONTAINER_PROJECT_GOPATH} \
        -e CGO_ENABLED=0 \
        -e GODEBUG=netdns=go \
        -w "${CONTAINER_PROJECT_DIR}" \
        golang:1.7rc1-alpine \
        go build -v -o tracer-cli ${PROJECT_NAME}/cmd/tracer-cli

strip "${PROJECT_DIR}/tracer"
strip "${PROJECT_DIR}/tracer-cli"

docker build -f ${PROJECT_DIR}/Dockerfile \
    -t ${CONTAINER_NAME}:${CONTAINER_TAG} \
    --build-arg BINARY_FILE=./tracer \
    --build-arg CLI_BINARY_FILE=./tracer-cli \
    "${PROJECT_DIR}"

rm -f "${PROJECT_DIR}/tracer" "${PROJECT_DIR}/tracer-cli"
