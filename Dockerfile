# syntax=docker/dockerfile:1
FROM maven:3.8.8-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline || true
COPY src ./src
RUN mvn -q -DskipTests clean package

FROM eclipse-temurin:21-jre-jammy
RUN groupadd -r flowlens && useradd -r -g flowlens -u 1000 flowlens
WORKDIR /app
COPY --from=build /build/target/flowlens-0.1.0-SNAPSHOT.jar /app/flowlens.jar
RUN chown -R flowlens:flowlens /app
USER 1000:1000

ENV FLOWLENS_HOST=0.0.0.0
ENV FLOWLENS_PORT=8080
EXPOSE 8080

ENTRYPOINT ["java","-Xms64m","-Xmx160m","-XX:+UseG1GC","-XX:MaxMetaspaceSize=64m","-XX:ReservedCodeCacheSize=64m","-XX:+UseCodeCacheFlushing","-XX:MaxDirectMemorySize=16m","-XX:NativeMemoryTracking=summary","-Xss256k","-XX:ActiveProcessorCount=4","-jar","/app/flowlens.jar"]