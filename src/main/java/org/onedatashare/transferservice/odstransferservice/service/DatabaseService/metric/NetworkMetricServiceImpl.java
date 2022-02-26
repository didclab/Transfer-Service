package org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric;

import org.onedatashare.transferservice.odstransferservice.DataRepository.NetworkMetricRepository;
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
    public NetworkMetric saveOrUpdate(NetworkMetric networkMetric) {
        System.out.println("Save: "+networkMetric.toString());
        try {
            System.out.println("Saving");
            return repository.save(networkMetric);
        }
        catch (Exception ex) {
            ex.getMessage();
        }
        return null;
    }



}
