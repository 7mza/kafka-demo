plugins { id("com.github.davidmc24.gradle.plugin.avro") }

group = "com.hamza.kafka.commons"
version = "0.0.1"

val avroVersion = "1.12.1"
val hypersistenceTsidVersion = "2.1.4"

// any project that depends on BaseEntity/TSIDGenerator must pull data-jpa & validation as impl
dependencies {
    api("org.apache.avro:avro:$avroVersion")

    compileOnly("org.postgresql:postgresql")
    compileOnly("org.springframework.boot:spring-boot-starter-data-jpa")
    compileOnly("org.springframework.boot:spring-boot-starter-validation")
    compileOnly("org.springframework:spring-web")

    implementation("io.hypersistence:hypersistence-tsid:$hypersistenceTsidVersion")
}

tasks {
    bootJar { enabled = false }
    bootRun { enabled = false }
}

sourceSets {
    main { java.srcDir(files("build/generated-main-avro-java").builtBy("generateAvroJava")) }
    test { java.srcDir(files("build/generated-test-avro-java").builtBy("generateTestAvroJava")) }
}
