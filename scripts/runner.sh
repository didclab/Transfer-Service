#! /bin/sh

# Requires some aws stuff
export AWS_REGION=us-east-2
aws s3 cp s3://<boot-file-path> /app/config/
aws s3 cp s3://<certs-path> /app/certs/ --recursive
chmod 400 /app/certs/* 


## Source boot.sh from mounted directory

source /app/config/boot.sh

# Run transfer-service
java -jar /usr/local/lib/ods-transfer-service-0.0.1-SNAPSHOT.jar