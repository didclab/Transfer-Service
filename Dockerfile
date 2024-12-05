FROM maven:3.9.7-amazoncorretto-21 AS build

COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package -DskipTests

# Final Image
FROM amazoncorretto:21-alpine3.18-jdk

RUN apk update
RUN apk --no-cache add python3-dev py3-pip build-base gcc linux-headers

RUN pip install pmeter-ods

COPY --from=build /home/app/target/ods-transfer-service-0.0.1-SNAPSHOT.jar /usr/local/lib/ods-transfer-service-0.0.1-SNAPSHOT.jar

ENV PIP_ROOT_USER_ACTION=ignore

#change to monitor the active NIC
ENV PMETER_CLI_OPTIONS="-NS"
ENV PMETER_NIC_INTERFACE="${PMETER_NIC_INTERFACE:-eth0}"
ENV ENABLE_PMETER="true"
ENV PMETER_CRON_EXP="*/15 * * * * *"
ENV SPRING_PROFILES_ACTIVE=aws,virtual,cockroach

ENV PATH "/home/ods/.local/bin:${PATH}"

RUN mkdir -p $HOME/.pmeter/
RUN touch $HOME/.pmeter/transfer_service_pmeter_measure.txt

EXPOSE 8092
ENTRYPOINT ["java","-jar", "/usr/local/lib/ods-transfer-service-0.0.1-SNAPSHOT.jar"]