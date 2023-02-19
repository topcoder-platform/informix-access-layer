FROM maven:3.8.6-openjdk-18 as builder
COPY src /home/tc-informix-dal/src
COPY pom.xml /home/tc-informix-dal
RUN mvn -f /home/tc-informix-dal/pom.xml clean
RUN mvn -f /home/tc-informix-dal/pom.xml package


FROM gcr.io/distroless/java:17
COPY --from=builder /home/tc-informix-dal/target/*.jar /app/informix-access-layer.jar
ENTRYPOINT ["java", "-jar", "/app/informix-access-layer.jar"]