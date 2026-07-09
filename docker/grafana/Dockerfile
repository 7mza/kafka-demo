FROM grafana/grafana:latest
USER root
RUN apk update
RUN apk upgrade
RUN apk add jq
RUN apk cache clean
RUN rm -rf /var/cache/apk/*
# download dashboards direclty in default dir provider
RUN mkdir -p /var/lib/grafana/dashboards
RUN wget --no-check-certificate -O /var/lib/grafana/dashboards/19004.json https://grafana.com/api/dashboards/19004/revisions/latest/download
# force any dynamic datasource to default datasource uid
RUN sed -i 's/${DS_PROMETHEUS}/prometheus_uid/g' /var/lib/grafana/dashboards/*.json
RUN sed -i 's/${DS_PROM}/prometheus_uid/g' /var/lib/grafana/dashboards/*.json
USER grafana
