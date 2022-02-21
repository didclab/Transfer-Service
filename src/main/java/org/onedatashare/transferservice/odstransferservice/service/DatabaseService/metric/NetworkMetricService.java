package org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric;

import org.onedatashare.transferservice.odstransferservice.cron.metric.NetworkMetric;

/**
 * @author deepika
 */
public interface NetworkMetricService {

    NetworkMetric save(NetworkMetric networkMetric);

}
