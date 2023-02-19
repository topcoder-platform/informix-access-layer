FROM maven:3.8.6-openjdk-18 as builder
COPY src /home/tc-informix-dal/src
COPY pom.xml /home/tc-informix-dal
COPY tc-dal-rdb-proto-1.0-SNAPSHOT.jar /home/tc-informix-dal
RUN mvn install:install-file -Dfile=/home/tc-informix-dal/tc-dal-rdb-proto:jar:1.0-SNAPSHOT -DgroupId=com.topcoder -DartifactId=tc-dal-rdb-proto -Dversion=1.0-SNAPSHOT -Dpackaging=jar
RUN mvn -f /home/tc-informix-dal/pom.xml clean
RUN mvn -f /home/tc-informix-dal/pom.xml package


FROM gcr.io/distroless/java:17
COPY --from=builder /home/tc-informix-dal/target/*.jar /app/informix-access-layer.jar
ENTRYPOINT ["java", "-jar", "/app/informix-access-layer.jar"]