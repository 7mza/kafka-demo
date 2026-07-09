import com.bmuschko.gradle.docker.tasks.image.DockerPushImage
import org.springframework.boot.gradle.tasks.aot.ProcessAot
import org.springframework.boot.gradle.tasks.aot.ProcessTestAot

plugins {
    id("com.bmuschko.docker-remote-api")
    id("com.google.cloud.tools.jib")
    id("org.graalvm.buildtools.native")
}

group = "com.hamza.kafka.order"
version = "0.0.1"

private val avroSerializerVersion = "8.3.0"
private val datasourceMicrometerVersion = "2.2.1"
private val logbookSpringVersion = "4.0.4"
private val openapiVersion = "3.0.3"
private val preLiquibaseVersion = "2.0.0"

dependencies {
    developmentOnly("org.springframework.boot:spring-boot-docker-compose")

    implementation("io.confluent:kafka-avro-serializer:$avroSerializerVersion")
    implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:$openapiVersion")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-restclient")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.zalando:logbook-spring-boot-starter:$logbookSpringVersion")
    implementation(project(":commons"))

    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    runtimeOnly("net.lbruun.springboot:preliquibase-spring-boot-starter:$preLiquibaseVersion")
    runtimeOnly("net.ttddyy.observation:datasource-micrometer-spring-boot:$datasourceMicrometerVersion")
    runtimeOnly("org.ehcache:ehcache::jakarta")
    runtimeOnly("org.hibernate.orm:hibernate-jcache")
    runtimeOnly("org.hibernate.orm:hibernate-micrometer")
    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("org.springframework.boot:spring-boot-starter-actuator")
    runtimeOnly("org.springframework.boot:spring-boot-starter-liquibase")
    runtimeOnly("org.springframework.boot:spring-boot-starter-opentelemetry")

    testImplementation("org.springframework.boot:spring-boot-starter-data-jpa-test")
    testImplementation("org.springframework.boot:spring-boot-starter-kafka-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation(testFixtures(project(":commons")))
}

private val dockerRegistry = property("dockerRegistry") as String
private val dockerUsername = property("dockerUsername") as String
private val dockerImage = "$dockerRegistry/$dockerUsername/${project.name}"

tasks {
    withType<Test>().configureEach {
        // https://graalvm.github.io/native-build-tools/latest/gradle-plugin.html
        if (project.hasProperty("generateMetadata")) {
            val metadataDir = "$projectDir/src/main/resources/META-INF/native-image/"
            doFirst {
                delete(
                    file("$metadataDir/.lock"),
                    file("$metadataDir/reachability-metadata.json"),
                )
            }
            jvmArgs("-agentlib:native-image-agent=config-merge-dir=$metadataDir")
            maxParallelForks = 1
            forkEvery = 0
        }
    }

    withType<ProcessAot>().configureEach { args("--spring.profiles.active=default,container") }

    withType<ProcessTestAot>().configureEach { jvmArgs("-XX:+EnableDynamicAgentLoading") }

    register<Exec>("buildImage") {
        description = "build image using buildx"
        group = "publishing"
        workingDir(rootDir)
        commandLine(
            "docker",
            "buildx",
            "build",
            "--load",
            "--build-arg",
            "MODULE_NAME=${project.name}",
            "-f",
            "Dockerfile",
            "-t",
            "$dockerImage:${project.version}",
            "-t",
            "$dockerImage:latest",
            ".",
        )
    }

    register<DockerPushImage>("publishImage") {
        description = "publish image to DockerHub"
        group = "publishing"
        images.set(setOf("$dockerImage:${project.version}", "$dockerImage:latest"))
        registryCredentials {
            username.set(dockerUsername)
            password.set(System.getenv("DOCKERHUB_TOKEN") ?: "")
        }
    }
}

springBoot { buildInfo {} }

graalvmNative { binaries { named("main") { buildArgs.addAll("--static", "--libc=musl", "-Os") } } }

jib {
    from { image = "eclipse-temurin:25-jre-alpine" }
    to { tags = setOf("latest") }
    container { ports = listOf("80") }
}

tasks.jibDockerBuild { dependsOn(tasks.build) }
