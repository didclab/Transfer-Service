#! /bin/sh

# Requires some aws stuff
export AWS_REGION=us-east-2
aws s3 cp s3://<path-to-config>/config/boot.sh /app/config/
aws s3 cp s3://<path-to-certs>/certs /app/certs/ --recursive
chmod 600 /app/certs/* 
chmod u+x /app/config/boot.sh
ls -alR /app

## Source boot.sh from mounted directory
sed -i "s/<node>/t3_ec2_medium/g" /app/config/boot.sh
source /app/config/boot.sh

# Run transfer-service
java -jar /usr/local/lib/ods-transfer-service-0.0.1-SNAPSHOT.jar