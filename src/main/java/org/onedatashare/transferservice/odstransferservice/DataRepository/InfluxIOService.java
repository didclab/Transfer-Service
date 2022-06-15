package org.onedatashare.transferservice.odstransferservice.DataRepository;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.BucketRetentionRules;
import com.influxdb.client.domain.SchemaType;
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
import java.util.List;

@Service
public class InfluxIOService {

    @Autowired
    private InfluxDBClient influxDBClient;

    Logger logger = LoggerFactory.getLogger(InfluxIOService.class);

    @Value("${influxdb.bucket}")
    String bucketName;
    @Value("${influxdb.org}")
    String org;

    private static final Logger LOG = LoggerFactory.getLogger(InfluxIOService.class);

    @PostConstruct
    public void postConstruct(){
        Bucket bucket = influxDBClient.getBucketsApi().findBucketByName(bucketName);
        if(bucket == null){
            logger.info("Creating the Influx bucket name={}, org={}", bucketName, org);
            try{
                bucket = this.influxDBClient.getBucketsApi().createBucket(bucketName, org);
            }catch(UnprocessableEntityException ignored){}
        }
    }


    public void insertDataPoints(List<DataInflux> data) {
        WriteApiBlocking writeApiBlocking = this.influxDBClient.getWriteApiBlocking();
        try {
            writeApiBlocking.writeMeasurements(WritePrecision.MS, data);
        } catch (InfluxException exception) {
            LOG.error("Exception occurred while pushing measurement to influx: " + exception.getMessage());
        }
    }
}