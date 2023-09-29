package org.onedatashare.transferservice.odstransferservice.model;

import com.influxdb.annotations.Column;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * @author deepika
 */
@Entity
@Table(name = "network_metric")
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
@Component
public class NetworkMetric {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "data")
    private String data;

    @Column
    private Date startTime;

    @Column
    private Date endTime;

    @Transient
    private JobMetric jobData;

}
