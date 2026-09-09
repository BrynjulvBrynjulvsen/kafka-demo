# ADR-00001: Use Kotlin, Spring Boot, and Spring Kafka

Date: 2026-09-07

Status: Accepted

## Context

The backend must produce and consume real Kafka records and serve browser clients. Kotlin is a project requirement, with an initial preference for Spring Boot and Spring Kafka. The source itself must be suitable for teaching.

## Decision

Use one Kotlin Spring Boot application with Spring MVC, Spring WebSocket, and Spring Kafka. Use `KafkaTemplate` for production and an annotated listener for consumption. Build with the Gradle wrapper and a JDK 17 toolchain; let Spring Boot manage library dependency versions where possible.

Keep the producer controller, consumer listener, configuration, and WebSocket transport in small, separate classes. Do not introduce a general backend plugin framework in the first increment.

## Alternatives

- Ktor with raw Kafka clients would provide a smaller web framework but require more consumer lifecycle integration.
- A fully reactive backend would introduce another programming model before a lesson requires it.
- Raw Kafka clients throughout would make every lifecycle detail explicit but increase initial scaffolding.

## Consequences

The initial implementation follows the owner's preference and uses familiar Kafka-facing abstractions. Spring owns polling, consumer lifecycle, and commit coordination; lessons about those mechanisms must explain the abstraction or introduce a more explicit example. Backend framework choices do not select the frontend stack.
