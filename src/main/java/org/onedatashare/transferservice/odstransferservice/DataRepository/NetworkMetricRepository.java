package org.onedatashare.transferservice.odstransferservice.DataRepository;

import org.onedatashare.transferservice.odstransferservice.cron.metric.NetworkMetric;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

/**
 * @author deepika
 */
public interface NetworkMetricRepository extends JpaRepository<NetworkMetric, Long> {

}
