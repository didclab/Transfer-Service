package org.onedatashare.transferservice.odstransferservice.DataRepository;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.exceptions.InfluxException;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class InfluxIOService {

    @Autowired
    private InfluxDBClient influxDBClient;

    private static final Logger LOG = LoggerFactory.getLogger(InfluxIOService.class);

    public void insertDataPoints(List<DataInflux> data) {
        WriteApiBlocking writeApiBlocking = this.influxDBClient.getWriteApiBlocking();
        try {
            writeApiBlocking.writeMeasurements(WritePrecision.MS, data);
        } catch (InfluxException exception) {
            LOG.error("Exception occurred while pushing measurement to influx: " + exception.getMessage());
        }
    }
}