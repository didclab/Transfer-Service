package org.onedatashare.transferservice.odstransferservice.DataRepository;


import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.exceptions.InfluxException;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.onedatashare.transferservice.odstransferservice.config.InfluxConfig;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.onedatashare.transferservice.odstransferservice.model.metrics.NetworkMetricInflux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

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
            System.out.println("Exception: "+exception);
        }
    }

    public Boolean insertDataPoints(DataInflux data){

        if(influxDBClient== null){
            instantiateInfluxClient();
        }
        Boolean flag= false;
        try{
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            writeApi.writeMeasurement(WritePrecision.MS, data);
            flag= true;
        }
        catch (InfluxException exception){
            System.out.println("Exception: "+exception.getMessage());
        }
        finally {
            return flag;
        }
    }

    public void fetch(){
        String query = "from(bucket: \"network_data\") |> range(start: -1h)";
        List<FluxTable> tables = influxDBClient.getQueryApi().query(query, influxConfig.getOrg());

        for (FluxTable table : tables) {
            for (FluxRecord record : table.getRecords()) {
                System.out.print(record.getField()+ " : "+ record.getValue()+ "; ");
            }
            System.out.println();
        }
    }

}