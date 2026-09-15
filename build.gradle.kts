plugins {
    base
    kotlin("jvm") version "2.3.21" apply false
    kotlin("plugin.spring") version "2.3.21" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}
allprojects {
    group = "io.bekk.kafkademo"
    version = "0.1.0-SNAPSHOT"
    repositories { mavenCentral() }
}
tasks.named("check") { dependsOn(":backend:check", ":presentation:check") }
tasks.named("assemble") { dependsOn(":backend:assemble", ":presentation:assemble") }
