FROM ghcr.io/graalvm/native-image-community:25i1-muslib AS builder
ENV JAVA_HOME=/usr/lib64/graalvm/graalvm-community-java25
RUN microdnf install -y --nodocs --setopt=install_weak_deps=0 xz && microdnf clean all
RUN curl -fsSL https://github.com/upx/upx/releases/download/v5.2.0/upx-5.2.0-amd64_linux.tar.xz \
    | tar -xJ --strip-components=1 -C /usr/local/bin/ upx-5.2.0-amd64_linux/upx

WORKDIR /app
COPY gradle/ gradle/
COPY build.gradle.kts gradle.properties gradlew settings.gradle.kts ./
COPY commons/build.gradle.kts commons/build.gradle.kts
COPY order-api/build.gradle.kts order-api/build.gradle.kts
RUN ./gradlew :order-api:dependencies -x npm_run_format --no-daemon

COPY commons/src/ commons/src/
COPY order-api/src/ order-api/src/
RUN ./gradlew :order-api:nativeCompile -x test -x npm_run_format --no-daemon

RUN upx --lzma --best order-api/build/native/nativeCompile/order-api

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=builder /app/order-api/build/native/nativeCompile/order-api /app
ENTRYPOINT ["/app"]
