plugins {
    `java-test-fixtures`
    id("com.github.davidmc24.gradle.plugin.avro")
}

group = "com.hamza.kafka.commons"
version = "0.0.1"

val avroVersion = "1.12.1"
val hypersistenceTsidVersion = "2.1.4"

// any project that depends on BaseEntity/TSIDGenerator must pull data-jpa & validation as impl
dependencies {
    api("org.apache.avro:avro:$avroVersion")

    compileOnly("org.apache.kafka:kafka-clients")
    compileOnly("org.postgresql:postgresql")
    compileOnly("org.springframework.boot:spring-boot-starter-data-jpa")
    compileOnly("org.springframework.boot:spring-boot-starter-validation")
    compileOnly("org.springframework.kafka:spring-kafka")
    compileOnly("org.springframework:spring-web")

    implementation("io.hypersistence:hypersistence-tsid:$hypersistenceTsidVersion")

    testFixturesApi("org.springframework.boot:spring-boot-testcontainers")
    testFixturesApi("org.testcontainers:testcontainers-kafka")
    testFixturesApi("org.testcontainers:testcontainers-postgresql")
    testFixturesApi("org.testcontainers:testcontainers-toxiproxy")

    testFixturesImplementation("org.springframework.boot:spring-boot-test")
    testFixturesImplementation("org.springframework:spring-test")
}

tasks {
    bootJar { enabled = false }
    bootRun { enabled = false }
}

sourceSets {
    main { java.srcDir(files("build/generated-main-avro-java").builtBy("generateAvroJava")) }
    test { java.srcDir(files("build/generated-test-avro-java").builtBy("generateTestAvroJava")) }
    testFixtures { java.srcDir(files("build/generated-testFixtures-avro-java").builtBy("generateTestFixturesAvroJava")) }
}
