package org.onedatashare.transferservice.odstransferservice.service.cron;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.DataRepository.NetworkMetricsInfluxRepository;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.model.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric.NetworkMetricServiceImpl;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.MetricCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.DoubleSummaryStatistics;
import java.util.Iterator;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.PMeterConstants;

/**
 * @author deepika
 */
@Service
@Getter
@Setter
public class MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);

    @Autowired
    private NetworkMetricServiceImpl networkMetricService;

    @Autowired
    private NetworkMetricsInfluxRepository repo;

    @Value("${pmeter.cron.run}")
    private boolean isCronEnabled;

    @Value("${job.metrics.save}")
    private boolean isJobMetricCollectionEnabled;

    private String outputFile = PMeterConstants.PMETER_REPORT_FOLDER + "pmeter-";

    JobMetric runningJobMetric;

    @Autowired
    MetricCache metricCache;

    @Autowired
    InfluxCache influxCache;

    @Autowired
    ThreadPoolManager threadPoolManager;

    JobMetric previousParentMetric;

    /**
     * Job controller which executes the cli script based on the configured cron expression,
     * maps and pushes the data in influx
     * <p>
     * 1. Execute pmeter script
     * 2. Read file
     * 3. Push to db
     */
    @Scheduled(cron = "${pmeter.cron.expression}")
    @SneakyThrows
    public void collectAndSave() {
        if (!isCronEnabled) return;
        networkMetricService.executeScript(null);
        //if we wish to save in synch uncomment following
        List<NetworkMetric> networkMetrics = networkMetricService.readFile(null);
        if (CollectionUtils.isEmpty(networkMetrics)) {
            //we don't want metric collection to break job execution, just returning without an exception
            return;
        }
        DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
        if (this.previousParentMetric == null) {
            this.previousParentMetric = new JobMetric();
        }

        Iterator itr = this.influxCache.threadCache.iterator();
        JobMetric parentJobMetric = this.previousParentMetric;
        while (itr.hasNext()) {
            JobMetric jobMetric = (JobMetric) itr.next();
            parentJobMetric = jobMetric;
            stats.accept(jobMetric.getThroughput());
        }

        parentJobMetric.setThroughput(stats.getAverage());
        if (this.previousParentMetric.getStepExecution() != null) {
            if (!this.previousParentMetric.getStepExecution().getJobExecution().isRunning()) {
                this.previousParentMetric.setConcurrency(0);
                this.previousParentMetric.setParallelism(0);
                this.previousParentMetric.setPipelining(0);
            }
        } else {
            this.previousParentMetric.setConcurrency(0);
            this.previousParentMetric.setParallelism(0);
            this.previousParentMetric.setPipelining(0);
        }
        this.influxCache.clearCache();
        networkMetrics.get(networkMetrics.size() - 1).setJobData(parentJobMetric); //why do we not set it on all?
        List<DataInflux> dataInflux = networkMetricService.mapData(networkMetrics);
        dataInflux.forEach(dataInflux1 -> log.info("Pushing DataInflux {}", dataInflux1));
        repo.insertDataPoints(dataInflux);
        this.previousParentMetric = parentJobMetric;
    }

    public void calculateThroughputAndSave(StepExecution stepExecution, double throughput, int size) {
        if (!isJobMetricCollectionEnabled) return;
        influxCache.addMetric(throughput, stepExecution, size);
    }

}
