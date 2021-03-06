FROM maven:3.6.3-jdk-11 AS build

COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package -DskipTests

FROM openjdk:11-jre-slim
COPY --from=build /home/app/target/ods-transfer-service-0.0.1-SNAPSHOT.jar /usr/local/lib/ods-transfer-service-0.0.1-SNAPSHOT.jar
EXPOSE 8083
ENTRYPOINT ["java","-jar","/usr/local/lib/ods-transfer-service-0.0.1-SNAPSHOT.jar"]

