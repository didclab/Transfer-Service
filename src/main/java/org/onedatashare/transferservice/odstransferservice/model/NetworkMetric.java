package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.Date;
import org.springframework.stereotype.Component;

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
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private Long id;

    @Column(name = "data")
    private String data;

    @Column
    private Date startTime;

    @Column
    private Date endTime;

    @Column
    private String jobData;
    
}
