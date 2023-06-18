FROM maven:3.6.3-jdk-11 AS build

COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package -DskipTests

FROM alpine:3.16.2 as pmeter-build
RUN apk add --update --no-cache python3-dev py3-pip build-base gcc linux-headers && \
    ln -sf python3 /usr/bin/python && python -m ensurepip \
    && pip3 install --upgrade pip setuptools wheel \
    && rm -r /usr/lib/python*/ensurepip && \
    if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi && \
    rm -r /root/.cache

RUN cd $HOME && pip install pmeter_ods==1.0.8 --user

ARG ALPINE_VERSION=3.16

# Final Image
FROM alpine:3.16.2
RUN apk --no-cache add openjdk11
#RUN apk --no-cache add openjdk11 python3-dev py3-pip build-base gcc linux-headers

RUN #pip install pmeter-ods==1.0.8

RUN #apk del build-base

COPY --from=build /home/app/target/ods-transfer-service-0.0.1-SNAPSHOT.jar /usr/local/lib/ods-transfer-service-0.0.1-SNAPSHOT.jar
COPY --from=pmeter-build --chown=ods:ods /root/.local /home/ods/.local
COPY ./boot.sh /home/app

RUN #chown -R ods:ods /app && chmod u+x /app/scripts/runner.sh
ENV NODE_NAME="${NODE_NAME}"
ENV USER_NAME="${USER_NAME}"
ENV APP_NAME="${USER_NAME}"-"${NODE_NAME}"

ENV CONNECTOR_QUEUE="${APP_NAME}"
ENV ODS_GDRIVE_CLIENT_ID="${ODS_GDRIVE_CLIENT_ID}"
ENV ODS_GDRIVE_CLIENT_SECRET="${ODS_GDRIVE_CLIENT_SECRET}"
ENV ODS_GDRIVE_PROJECT_ID="onedatashare-dev"
ENV EUREKA_URI="${EUREKA_URI}"
ENV EUREKA_PASS="${EUREKA_PASS}"
ENV EUREKA_USER="${EUREKA_USER}"
ENV FOLDER_WITH_CERTS="${FOLDER_WITH_CERTS}"
COPY ${FOLDER_WITH_CERTS} /certs/
ENV COCKROACH_URI="${COCKROACH_URI}"
ENV COCKROACH_USER="${COCKROACH_USER}"
ENV COCKROACH_PASS="${COCKROACH_PASS}"
ENV RMQ_ADDRESS="amqps://b-0e720b16-3ea7-4227-ad65-6cce3704121c.mq.us-east-2.amazonaws.com:5671"

#use ODS user for your private queue.
#create creds through aws console
ENV AMPQ_USER="${AMPQ_USER}"
ENV AMPQ_PWD="${AMPQ_PWD}"

#change to monitor the active NIC
ENV PMETER_NIC_INTERFACE="${PMETER_NIC_INTERFACE:-eth0}"
ENV INFLUX_ORG="${INFLUX_ORG}"
ENV INFLUX_BUCKET="${USER_NAME}"
ENV INFLUX_TOKEN="${INFLUX_TOKEN}"
ENV INFLUX_URI="https://influxdb.onedatashare.org"
ENV ENABLE_PMETER="true"
ENV PMETER_CRON_EXP="*/15 * * * * *"

ENV OPTIMIZER_URL="${OPTIMIZER_URL}"
ENV OPTIMIZER_ENABLE="${OPTIMIZER_ENABLE}"
ENV SPRING_PROFILE="${SPRING_PROFILE:-hsql}"

ENV PATH "/home/ods/.local/bin:${PATH}"
EXPOSE 8092
ENTRYPOINT ["java", "-Dspring.profiles.active=hsql","-jar", "/usr/local/lib/ods-transfer-service-0.0.1-SNAPSHOT.jar"]