group = "com.hamza.kafka.commons"
version = "0.0.1"

val hypersistenceTsidVersion = "2.1.4"

dependencies {
    implementation("io.hypersistence:hypersistence-tsid:$hypersistenceTsidVersion")
    // any project that depends on BaseEntity/TSIDGenerator must pull data-jpa & validation as impl
    compileOnly("org.springframework.boot:spring-boot-starter-data-jpa")
    compileOnly("org.springframework.boot:spring-boot-starter-validation")
    compileOnly("org.springframework:spring-web")
}

tasks {
    bootJar { enabled = false }
    bootRun { enabled = false }
}
