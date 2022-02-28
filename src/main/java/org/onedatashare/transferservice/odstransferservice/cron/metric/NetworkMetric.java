package org.onedatashare.transferservice.odstransferservice.cron.metric;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
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
public class NetworkMetric {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private Long id;

    @Column(name = "data")
    private String data;

    @Column
    private Date startTime;

    @Column
    private Date endTime;
    
}
