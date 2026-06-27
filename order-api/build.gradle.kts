import com.bmuschko.gradle.docker.tasks.image.DockerPushImage
import org.springframework.boot.gradle.tasks.aot.ProcessAot
import org.springframework.boot.gradle.tasks.aot.ProcessTestAot

plugins {
    id("com.bmuschko.docker-remote-api")
    id("org.graalvm.buildtools.native")
}

group = "com.hamza.kafka.order"
version = "0.0.1"

val logbookSpringVersion = "4.0.4"
val openapiVersion = "3.0.3"

dependencies {
    developmentOnly("org.springframework.boot:spring-boot-devtools")
    developmentOnly("org.springframework.boot:spring-boot-docker-compose")

    implementation("org.ehcache:ehcache::jakarta")
    implementation("org.hibernate.orm:hibernate-jcache")
    implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:$openapiVersion")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-liquibase")
    implementation("org.springframework.boot:spring-boot-starter-restclient")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.zalando:logbook-spring-boot-starter:$logbookSpringVersion")
    implementation(project(":commons"))

    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-data-jpa-test")
    testImplementation("org.springframework.boot:spring-boot-starter-kafka-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.testcontainers:testcontainers-postgresql")
}

val dockerRegistry = property("dockerRegistry") as String
val dockerUsername = property("dockerUsername") as String
val dockerImage = "$dockerRegistry/$dockerUsername/${project.name}"

tasks {
    withType<Test>().configureEach {
        // https://graalvm.github.io/native-build-tools/latest/gradle-plugin.html
        if (project.hasProperty("generateMetadata")) {
            val metadataDir = "$projectDir/src/main/resources/META-INF/native-image/"
            doFirst { delete(file("$metadataDir/reachability-metadata.json")) }
            jvmArgs("-agentlib:native-image-agent=config-merge-dir=$metadataDir")
            maxParallelForks = 1
            forkEvery = 0
        }
    }

    withType<ProcessAot>().configureEach {
        args("--spring.profiles.active=default,container")
        jvmArgs("-Dorg.jboss.logging.provider=slf4j")
    }

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
            "-f",
            "order-api/Dockerfile",
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
