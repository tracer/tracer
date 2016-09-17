FROM alpine:3.4
MAINTAINER Dominik Honnef "dominik@honnef.co"
MAINTAINER Florin Pățan "florinpatan@gmail.com"

ARG BINARY_FILE
ARG CLI_BINARY_FILE

# DNS stuff
RUN echo 'hosts: files mdns4_minimal [NOTFOUND=return] dns mdns4' >> /etc/nsswitch.conf

# SSL certs
RUN apk add --update ca-certificates \
    && rm -rf /var/cache/apk/*

# Binary
ADD cmd/tracer/example.conf /tracer/conf/app.conf
ADD $BINARY_FILE /tracer/tracer
ADD $CLI_BINARY_FILE /tracer/tracer-cli

# Runtime
EXPOSE 9411 9998 9999
CMD ["/tracer/tracer -c /tracer/conf/app.conf"]
