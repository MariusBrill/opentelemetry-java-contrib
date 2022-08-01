plugins {
  id("otel.java-conventions")
}

description = "An example OpenTelemetry Java Contrib library"

dependencies {
  implementation("com.influxdb:influxdb-client-java:6.3.0")
  implementation("io.opentelemetry:opentelemetry-sdk:1.16.0")
  implementation("io.opentelemetry:opentelemetry-exporter-otlp-common:1.15.0")
  implementation("io.opentelemetry:opentelemetry-api:1.16.0")



}

tasks {
  jar {
    manifest {
      attributes["Main-Class"] = "io.opentelemetry.contrib.example.Library"
    }
  }
}
