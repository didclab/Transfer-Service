package org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric;

import org.onedatashare.transferservice.odstransferservice.model.NetworkMetric;

import java.util.List;

/**
 * @author deepika
 */
public interface NetworkMetricService {
    NetworkMetric saveOrUpdate(NetworkMetric networkMetric);
}
