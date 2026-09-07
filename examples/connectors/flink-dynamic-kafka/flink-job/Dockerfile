# syntax=docker/dockerfile:1
FROM maven:3.9.16-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests package

FROM flink:2.3.0-java17
COPY --from=build /src/target/flink-dynamic-kafka.jar /opt/flink/usrlib/flink-dynamic-kafka.jar
