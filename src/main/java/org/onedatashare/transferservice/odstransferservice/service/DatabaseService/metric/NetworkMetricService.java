package org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric;

import org.onedatashare.transferservice.odstransferservice.cron.metric.NetworkMetric;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author deepika
 */
public interface NetworkMetricService {
    NetworkMetric saveOrUpdate(NetworkMetric networkMetric);
}
