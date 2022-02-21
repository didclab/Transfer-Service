package org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric;

import org.onedatashare.transferservice.odstransferservice.cron.NetworkMetricRepository;
import org.onedatashare.transferservice.odstransferservice.cron.metric.NetworkMetric;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author deepika
 */
@Service
public class NetworkMetricServiceImpl implements NetworkMetricService {

    @Autowired
    NetworkMetricRepository repository;
    @Override
    public NetworkMetric save(NetworkMetric networkMetric) {
        NetworkMetric res = repository.save(networkMetric);
        System.out.println(res.toString());
        return res;
    }
}
