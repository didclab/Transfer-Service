export EUREKA_URI=3.136.74.174:8090
export EUREKA_PASS=hE110K1ttEns
export EUREKA_USER=admin
export COCKROACH_URI="postgresql://18.222.165.68:26257/job_details?sslcert=certs/client.odsdev.crt&sslkey=certs/client.odsdev.key&sslmode=verify-full&sslrootcert=certs/ca.crt"
#export COCKROACH_URI="postgresql://18.222.165.68:26257/job_details?sslcert=client.odsdev.crt&sslkey=client.odsdev.key&sslmode=verify-full&sslrootcert=ca.crt"
#export COCKROACH_USER=odsDev
#export COCKROACH_PASS=changeme123
export CONNECTOR_QUEUE=transferQueue
export COCKROACH_USER=temp
export COCKROACH_PASS=meatIsMurder
export RMQ_ADDRESS=amqps://b-0e720b16-3ea7-4227-ad65-6cce3704121c.mq.us-east-2.amazonaws.com:5671
export AMPQ_PORT=
export AMPQ_USER=transferservice
export AMPQ_PWD="9*7#z^f8v*UtWbf"
export SFTP_SWITCH=1
mvn clean package -DskipTests
java -jar target/ods-transfer-service-0.0.1-SNAPSHOT.jar
