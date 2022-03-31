package org.onedatashare.transferservice.odstransferservice.DataRepository;


import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.exceptions.InfluxException;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.onedatashare.transferservice.odstransferservice.model.NetworkMetricInflux;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class NetworkMetricsInfluxRepository {
    @Value("${influxdb.token}")
    private String token= "w5KTJ4R9sE4cRrscCEzk6yVl97_8_ZqcjMmGhky6sdERpfqyX0t-FxZjM2qCwQVqElDchiH_LKgTFAc0K8e0dw==";

    @Value("${influxdb.url}")
    private String url= "http://localhost:8086";

    @Value("${influxdb.bucket}")
    private String bucket= "network_data";

    @Value("${influxdb.org}")
    private String org= "suny";

    private InfluxDBClient influxDBClient;

    public void instantiateInfluxClient(){
        try {
            influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray());
        }
        catch (Exception exception){
            System.out.println("Exception: "+exception.getMessage());
        }
    }

    public Boolean insertDataPoints(NetworkMetricInflux metric){
        if(influxDBClient== null){
            instantiateInfluxClient();
        }
        Boolean flag= false;
        try{
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            writeApi.writeMeasurement(bucket, org, WritePrecision.MS, metric);
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
        List<FluxTable> tables = influxDBClient.getQueryApi().query(query, org);

        for (FluxTable table : tables) {
            for (FluxRecord record : table.getRecords()) {
                System.out.print(record.getField()+ " : "+ record.getValue()+ "; ");
            }
            System.out.println();
        }
    }

}