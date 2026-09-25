import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    application
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

group = "example.monitoring"
version = "0.1.0"

val verifyAkkaAccess = tasks.register("verifyAkkaAccess") {
    group = "verification"
    description = "Checks that the private Akka repository has been configured."
    val repositoryUrl = providers.environmentVariable("AKKA_REPOSITORY_URL")
        .orElse(providers.gradleProperty("akkaRepositoryUrl"))
    doLast {
        check(!repositoryUrl.orNull.isNullOrBlank()) {
            "Akka access is not configured. Set AKKA_REPOSITORY_URL or the user-level " +
                "Gradle property akkaRepositoryUrl. Never commit or share its token."
        }
        check(repositoryUrl.get().startsWith("https://")) {
            "The Akka repository must use HTTPS."
        }
    }
}

kotlin {
    jvmToolchain(21)
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)
        allWarningsAsErrors.set(true)
    }
}

application {
    mainClass.set("example.monitoring.MainKt")
}

dependencies {
    implementation(platform(libs.akka.bom))
    implementation(libs.akka.actor.typed)
    implementation(libs.akka.stream)
    implementation(libs.akka.slf4j)
    implementation(libs.akka.http)
    implementation(libs.serialization.json)
    implementation(libs.typesafe.config)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}

tasks.named("compileKotlin") {
    dependsOn(verifyAkkaAccess)
}

tasks.test {
    useJUnitPlatform()
    maxParallelForks = 1
    testLogging { events("failed", "skipped") }
}
