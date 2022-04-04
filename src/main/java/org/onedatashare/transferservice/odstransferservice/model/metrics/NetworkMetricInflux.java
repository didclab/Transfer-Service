package org.onedatashare.transferservice.odstransferservice.model.metrics;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Date;

@Data
@Measurement(name= "network_data")
@Component
public class NetworkMetricInflux {

    @Column(name= "time")
    private Instant time;

    @Column(name= "data")
    private DataInflux[] data;

    @Column(name = "start_time")
    private Date start_time;

    @Column(name = "end_time")
    private Date end_time;

}
