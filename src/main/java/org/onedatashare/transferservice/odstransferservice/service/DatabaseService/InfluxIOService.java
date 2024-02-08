package org.onedatashare.transferservice.odstransferservice.service.DatabaseService;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.exceptions.InfluxException;
import com.influxdb.exceptions.UnprocessableEntityException;
import jakarta.annotation.PostConstruct;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class InfluxIOService {

    private final InfluxDBClient influxClient;
    Logger logger = LoggerFactory.getLogger(InfluxIOService.class);

    @Value("${ods.influx.bucket}")
    private String bucketName;

    @Value("${ods.influx.org}")
    String org;

    Bucket bucket;
    private WriteApi writeApi;

    public InfluxIOService(InfluxDBClient influxClient) {
        this.influxClient = influxClient;
        this.writeApi = this.influxClient.makeWriteApi();
    }

    @PostConstruct
    public void postConstruct() {
        this.reconfigureBucketForNewJob(this.bucketName);
    }

    public void reconfigureBucketForNewJob(String ownerId) {
        logger.info("********* Reconfiguring the Bucket ***********");
        if (ownerId == null) {
            bucket = influxClient.getBucketsApi().findBucketByName(this.bucketName);
        } else {
            bucket = influxClient.getBucketsApi().findBucketByName(ownerId);
        }

        if (bucket == null) {
            logger.info("Creating the Influx bucket name={}, org={}", ownerId, org);
            try {
                bucket = this.influxClient.getBucketsApi().createBucket(ownerId, org);
            } catch (UnprocessableEntityException ignored) {
            }
        }
    }


    public void insertDataPoint(DataInflux point) {
        try {
            writeApi.writeMeasurement(this.bucket.getName(), this.org, WritePrecision.MS, point);
        } catch (InfluxException exception) {
            logger.error("Exception occurred while pushing measurement to influx: " + exception.getMessage());
        }
    }
}