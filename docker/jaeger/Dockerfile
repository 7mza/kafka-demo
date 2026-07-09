FROM jaegertracing/jaeger:latest
RUN apk update
RUN apk upgrade
RUN apk add jq
RUN apk cache clean
RUN rm -rf /var/cache/apk/*
