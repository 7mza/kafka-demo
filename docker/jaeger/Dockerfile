FROM jaegertracing/jaeger:latest
USER root
RUN apk update
RUN apk upgrade
RUN apk add jq
RUN apk cache clean
RUN rm -rf /var/cache/apk/*
USER 10001
