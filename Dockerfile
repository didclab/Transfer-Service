FROM openjdk:8-jdk-alpine
RUN apk update && apk add bash
RUN mkdir /app
WORKDIR /app
EXPOSE 8092
COPY target/ods-transfer-service-0.0.1-SNAPSHOT.jar .
ENTRYPOINT ["java","-jar","/app/ods-transfer-service-0.0.1-SNAPSHOT.jar"]