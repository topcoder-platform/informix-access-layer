FROM maven:3.8.6-openjdk-18 as build
COPY src /home/tc-informix-dal/src
COPY pom.xml /home/tc-informix-dal
RUN mvn -f /home/tc-informix-dal/pom.xml clean package
