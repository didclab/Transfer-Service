package org.onedatashare.transferservice.odstransferservice.cron;

import org.onedatashare.transferservice.odstransferservice.cron.metric.NetworkMetric;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

/**
 * @author deepika
 */
@Repository
public interface NetworkMetricRepository extends CrudRepository<NetworkMetric, Long> {

}
