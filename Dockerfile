FROM ghcr.io/graalvm/native-image-community:25i1-muslib AS builder
ARG MODULE_NAME
ENV JAVA_HOME=/usr/lib64/graalvm/graalvm-community-java25
RUN microdnf install -y --nodocs --setopt=install_weak_deps=0 xz && microdnf clean all
RUN curl -fsSL https://github.com/upx/upx/releases/download/v5.2.0/upx-5.2.0-amd64_linux.tar.xz \
    | tar -xJ --strip-components=1 -C /usr/local/bin/ upx-5.2.0-amd64_linux/upx

WORKDIR /app
COPY gradle/ gradle/
COPY build.gradle.kts gradle.properties gradlew settings.gradle.docker ./
RUN cp settings.gradle.docker settings.gradle.kts && echo "include(\":$MODULE_NAME\")" >> settings.gradle.kts
COPY commons/build.gradle.kts commons/build.gradle.kts
COPY $MODULE_NAME/build.gradle.kts $MODULE_NAME/build.gradle.kts
RUN ./gradlew :$MODULE_NAME:dependencies -x npm_run_format --no-daemon

COPY commons/src/ commons/src/
COPY $MODULE_NAME/src/ $MODULE_NAME/src/
RUN ./gradlew :$MODULE_NAME:nativeCompile -x test -x npm_run_format --no-daemon

RUN upx --lzma --best $MODULE_NAME/build/native/nativeCompile/$MODULE_NAME

FROM gcr.io/distroless/static-debian13:nonroot
ARG MODULE_NAME
COPY --from=builder /app/$MODULE_NAME/build/native/nativeCompile/$MODULE_NAME /app
ENTRYPOINT ["/app"]
