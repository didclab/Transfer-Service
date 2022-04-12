package org.onedatashare.transferservice.odstransferservice.service.cron;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.DataRepository.NetworkMetricsInfluxRepository;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.model.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric.NetworkMetricServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * @author deepika
 */
@Service
@NoArgsConstructor
@Getter
@Setter
public class MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);

    @Autowired
    private NetworkMetricServiceImpl networkMetricService;

    @Autowired
    private NetworkMetricsInfluxRepository repo;

    /**
     *  Job controller which executes the cli script based on the configured cron expression,
     *  maps and pushes the data in influx
     *
     * 1. Execute pmeter script
     * 2. Read file
     * 3. Push to db
     *
     */
    @Scheduled(cron = "${pmeter.cron.expression}")
    @SneakyThrows
    public void collectAndSave() {
        networkMetricService.executeScript();
        NetworkMetric networkMetric = networkMetricService.readFile();
        if(networkMetric==null){
            throw new Exception("networkMetric must not be null");
        }
        //saveData(networkMetric);
        DataInflux dataInflux = networkMetricService.mapData(networkMetric);
        repo.insertDataPoints(dataInflux);
    }

    @SneakyThrows
    public void collectJobMetrics(){
        networkMetricService.executeScript();
        NetworkMetric networkMetric = networkMetricService.readFile();
        if(networkMetric==null){
            throw new Exception("networkMetric must not be null");
        }
        String jobData = "{'id':'12345', 'throughput':234.0}";
        networkMetric.setJobData(jobData);
        DataInflux dataInflux = networkMetricService.mapData(networkMetric);
        repo.insertDataPoints(dataInflux);
    }
}
