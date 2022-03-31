package org.onedatashare.transferservice.odstransferservice.model;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import lombok.Cleanup;
import lombok.Data;

import java.time.Instant;
import java.util.Date;

@Data
@Measurement(name= "network_data")
public class NetworkMetricInflux {

    @Column(name= "time")
    private Instant time;

    @Column(name= "data")
    private String data;

    @Column(name = "start_time")
    private Date start_time;

    @Column(name = "end_time")
    private Date end_time;

}
