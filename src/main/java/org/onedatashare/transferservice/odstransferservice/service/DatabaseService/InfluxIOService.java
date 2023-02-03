package org.onedatashare.transferservice.odstransferservice.service.DatabaseService;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.exceptions.InfluxException;
import com.influxdb.exceptions.UnprocessableEntityException;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class InfluxIOService {

    @Autowired
    private InfluxDBClient influxDBClient;

    Logger logger = LoggerFactory.getLogger(InfluxIOService.class);

    @Value("${ods.influx.bucket}")
    private String bucketName;
    @Value("${ods.influx.org}")
    String org;

    private static final Logger LOG = LoggerFactory.getLogger(InfluxIOService.class);
    private WriteApi writeApi;

    public InfluxIOService(InfluxDBClient influxDBClient) {
        this.influxDBClient = influxDBClient;
    }

    @PostConstruct
    public void postConstruct() {
        Bucket bucket = influxDBClient.getBucketsApi().findBucketByName(bucketName);
        if (bucket == null) {
            logger.info("Creating the Influx bucket name={}, org={}", bucketName, org);
            try {
                bucket = this.influxDBClient.getBucketsApi().createBucket(bucketName, org);
            } catch (UnprocessableEntityException ignored) {
            }
        }
        this.writeApi = this.influxDBClient.getWriteApi();
    }


    public void insertDataPoint(DataInflux point) {
        try {
            writeApi.writeMeasurement(WritePrecision.MS, point);
        } catch (InfluxException exception) {
            LOG.error("Exception occurred while pushing measurement to influx: " + exception.getMessage());
        }
    }
}