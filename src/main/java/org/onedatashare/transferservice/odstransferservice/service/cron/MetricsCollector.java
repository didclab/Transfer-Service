package org.onedatashare.transferservice.odstransferservice.service.cron;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.DataRepository.NetworkMetricsInfluxRepository;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.model.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric.NetworkMetricServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

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

    @Value("${pmeter.cron.run}")
    private boolean isCronEnabled;

    @Value("${job.metrics.save}")
    private boolean isJobMetricCollectionEnabled;

    private String outputFile = PMeterConstants.PMETER_REPORT_FOLDER + "pmeter-";

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
        log.info("Collecting network metrics");
        networkMetricService.executeScript(null);
        //if we wish to save in synch uncomment following
        log.info("Saving network metrics");
        List<NetworkMetric> networkMetrics = networkMetricService.readFile(null);
        if (CollectionUtils.isEmpty(networkMetrics)) {
            //we don't want metric collection to break job execution, just returning without an exception
            return;
        }
        List<DataInflux> dataInflux = networkMetricService.mapData(networkMetrics);
        repo.insertDataPoints(dataInflux);
    }

    @SneakyThrows
    public void collectJobMetrics(JobMetric jobMetric) {
        String outputFile = getOutputFile() + UUID.randomUUID() + ".txt";
        networkMetricService.executeScript(outputFile);
        List<NetworkMetric> networkMetrics = networkMetricService.readFile(outputFile);
        if (CollectionUtils.isEmpty(networkMetrics)) {
            return;
        }
        List<NetworkMetric> networkMetricList = new ArrayList<>();
        NetworkMetric networkMetric = new NetworkMetric();
        networkMetricList.add(networkMetric);
        networkMetricList.get(networkMetricList.size() - 1).setJobData(jobMetric);
        List<DataInflux> dataInflux = networkMetricService.mapData(networkMetricList);
        repo.insertDataPoints(dataInflux);
    }


    public JobMetric populateJobMetric(JobExecution jobExecution, StepExecution stepExecution) {
        JobParameters jobParameters = jobExecution.getJobParameters();
        JobMetric jobMetric = new JobMetric();
        jobMetric.setJobId(jobExecution.getJobId().toString());
        jobMetric.setConcurrency(jobParameters.getLong(CONCURRENCY));
        jobMetric.setParallelism(jobParameters.getLong(PARALLELISM));
        jobMetric.setPipelining(jobParameters.getLong(PIPELINING));
        jobMetric.setOwnerId(jobParameters.getString(APP_NAME));
        if (stepExecution == null) {
            long jobCompletionTime = Duration.between(jobExecution.getStartTime().toInstant(), jobExecution.getEndTime().toInstant()).toMillis();
            long size = jobParameters.getLong(JOB_SIZE, Long.valueOf(0));
            double throughput = (double) size / jobCompletionTime;
            log.info("total: " + size + " duration: " + jobCompletionTime);
            log.info("Job throughput (bytes/ms): " + throughput);
            jobMetric.setThroughput(throughput);
        } else {
            long duration = Duration.between(jobExecution.getStartTime().toInstant(), Instant.now()).toMillis();
            AtomicLong currentRead = (AtomicLong) stepExecution.getExecutionContext().get(ODSConstants.BYTES_READ);
            AtomicLong currentWrite = (AtomicLong) stepExecution.getExecutionContext().get(ODSConstants.BYTES_WRITTEN);
            if (currentRead == null) currentRead = new AtomicLong(0l);
            if (currentWrite == null) currentWrite = new AtomicLong(0l);
            log.info("read: " + currentRead + " duration: " + duration);
            log.info("write: " + currentWrite + " duration: " + duration);
            double throughput = (currentRead.doubleValue() + currentWrite.doubleValue()) / duration;
            throughput = Math.floor(throughput * 100) / 100;
            jobMetric.setThroughput(throughput);
        }
        log.info("Job metric: " + jobMetric);
        return jobMetric;
    }

    public void setBytes(StepExecution stepExecution, String key, Long bytesToAdd) {
        AtomicLong currentTp = (AtomicLong) stepExecution.getExecutionContext().get(key);
        if (currentTp == null) currentTp = new AtomicLong(0l);
        else currentTp.addAndGet(bytesToAdd);
        log.info("setting " + key + " : " + currentTp);
        stepExecution.getExecutionContext().put(key, currentTp);
    }

    public void calculateThroughputAndSave(StepExecution stepExecution, String key, Long bytes) {
        if (!isJobMetricCollectionEnabled) return;
        setBytes(stepExecution, key, Long.valueOf(bytes));
        JobMetric jobMetric = populateJobMetric(stepExecution.getJobExecution(), stepExecution);
        collectJobMetrics(jobMetric);
    }
}
