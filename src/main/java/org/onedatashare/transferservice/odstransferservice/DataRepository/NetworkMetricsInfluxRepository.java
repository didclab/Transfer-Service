package org.onedatashare.transferservice.odstransferservice.DataRepository;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.exceptions.InfluxException;
import org.onedatashare.transferservice.odstransferservice.config.InfluxConfig;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NetworkMetricsInfluxRepository {
    @Autowired
    private InfluxConfig influxConfig;

    private InfluxDBClient influxDBClient;

    private static final Logger LOG = LoggerFactory.getLogger(NetworkMetricRepository.class);

    public void instantiateInfluxClient(){
        try {
            InfluxDBClientOptions influxDBClientOptions= InfluxDBClientOptions.builder()
                    .url(influxConfig.getUrl())
                    .org(influxConfig.getOrg())
                    .bucket(influxConfig.getBucket())
                    .authenticateToken(influxConfig.getToken().toCharArray())
                    .build();

            influxDBClient = InfluxDBClientFactory.create(influxDBClientOptions);

        }
        catch (Exception exception){
            LOG.error("Exception occurred while connecting with influx: " + exception.getMessage());
        }
    }

    public void insertDataPoints(DataInflux data){
        if(influxDBClient== null){
            instantiateInfluxClient();
        }

        try{
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            writeApi.writeMeasurement(WritePrecision.MS, data);
            LOG.info("Successfully pushed measurement to influx at " + data.getEndTime());
        }
        catch (InfluxException exception){
            LOG.error("Exception occurred while pushing measurement to influx: " + exception.getMessage());
        }
    }

}