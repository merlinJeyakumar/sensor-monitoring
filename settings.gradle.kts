pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
        val akkaRepositoryUrl = providers.environmentVariable("AKKA_REPOSITORY_URL")
            .orElse(providers.gradleProperty("akkaRepositoryUrl"))
            .orNull
        if (!akkaRepositoryUrl.isNullOrBlank()) {
            exclusiveContent {
                forRepository {
                    maven {
                        name = "Akka"
                        url = uri(akkaRepositoryUrl)
                    }
                }
                filter {
                    includeGroup("com.typesafe.akka")
                }
            }
        }
    }
}

rootProject.name = "sensor-monitoring"
