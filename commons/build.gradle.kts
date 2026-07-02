group = "com.hamza.kafka.commons"
version = "0.0.1"

val hypersistenceTsidVersion = "2.1.4"

// any project that depends on BaseEntity/TSIDGenerator must pull data-jpa & validation as impl
dependencies {
    compileOnly("org.springframework.boot:spring-boot-autoconfigure")
    compileOnly("org.springframework.boot:spring-boot-jackson")
    compileOnly("org.springframework.boot:spring-boot-starter-data-jpa")
    compileOnly("org.springframework.boot:spring-boot-starter-validation")
    compileOnly("org.springframework:spring-web")

    implementation("io.hypersistence:hypersistence-tsid:$hypersistenceTsidVersion")

    testImplementation("org.assertj:assertj-core")
    testImplementation("org.slf4j:slf4j-api")
}

tasks {
    bootJar { enabled = false }
    bootRun { enabled = false }
}
