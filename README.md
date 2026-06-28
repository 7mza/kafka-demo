# Kafka demo

event-driven pipeline demo

## env

![Kraft cluster](docs/kafka_comp_demo.png)

this env can be modified using [compose.yaml](compose.yaml) and [.env](.env)

(in a real prod env it should be multiple isolation zones with N ctrls + M brokers in each zone)

## components

### commons

shared libs

### order-api

![order api sequence UML](docs/order_api_seq.png)

### inventory-service

TODO

### audit-service

TODO

## run

```shell
docker compose up --build
```

## build

[sdkman](https://sdkman.io)

[nvm](https://github.com/nvm-sh/nvm)

[docker](https://docs.docker.com/engine/install/)

```shell
nvm use && npm i && sdk env install
```

JVM

[uses spring compose support](compose.dev.yaml)

```shell
./gradlew clean ktlintFormat ktlintCheck build
./gradlew bootRun
```

Native

```shell
./gradlew clean ktlintFormat ktlintCheck build -PgenerateMetadata && ./gradlew --stop
./gradlew buildImage && ./gradlew --stop
docker compose up --build
```
