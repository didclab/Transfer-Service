package org.onedatashare.transferservice.odstransferservice.DataRepository;

import org.onedatashare.transferservice.odstransferservice.model.NetworkMetric;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author deepika
 */
public interface NetworkMetricRepository extends JpaRepository<NetworkMetric, Long> {

}
