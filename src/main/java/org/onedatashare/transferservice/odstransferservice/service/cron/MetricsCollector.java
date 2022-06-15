package org.onedatashare.transferservice.odstransferservice.service.cron;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.DataRepository.InfluxIOService;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.onedatashare.transferservice.odstransferservice.service.ConnectionBag;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.PmeterParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;


/**
 * @author deepika
 */
@Service
@Getter
@Setter
public class MetricsCollector {

    private Logger log = LoggerFactory.getLogger(MetricsCollector.class);

    @Autowired
    private InfluxIOService influxIOService;

    @Value("${pmeter.cron.run}")
    private boolean isCronEnabled;

    @Value("${job.metrics.save}")
    private boolean isJobMetricCollectionEnabled;

    @Value("${spring.application.name}")
    String appName;

    @Value("${ods.user}")
    String odsUser;

    @Autowired
    InfluxCache influxCache;

    @Autowired
    ThreadPoolManager threadPoolManager;

    @Autowired
    PmeterParser pmeterParser;

    JobMetric previousParentMetric;

    @Autowired
    ConnectionBag connectionBag;


    /**
     * This is not blocking to the transfer job as this is getting run by the thread that is processing the CRON.
     * BUT, lets say we are using more threads than the CPU can handle to run none stop concurrently(which we are).
     * Then this thread could potentially not be pre-empted by other threads thus slowing down the transfer by a super small amount
     * Why small? B/c the pmeter file is small, its a few json objects max.
     * Job controller which executes the cli script based on the configured cron expression,
     * maps and pushes the data in influx
     * <p>
     * <p>
     * 1. Execute pmeter script
     * 2. Read file
     * 3. Push to db
     */
    @Scheduled(cron = "${pmeter.cron.expression}")
    @SneakyThrows
    public void collectAndSave() {
        if (!isCronEnabled) return;
        pmeterParser.runPmeter();
        List<DataInflux> pmeterMetrics = pmeterParser.parsePmeterOutput();
        if (pmeterMetrics.size() < 1) return;

        this.previousParentMetric = influxCache.someJobMetric(); //this metrics throughput is the throughput of the whole map in influxCache.
        long jobSize = 0L;
        long avgFileSize = 0L;
        long pipeSize = 0L;
        if (this.previousParentMetric.getStepExecution() != null) {
            JobParameters jobParameters = this.previousParentMetric.getStepExecution().getJobParameters();
            jobSize = jobParameters.getLong(JOB_SIZE);
            avgFileSize = jobParameters.getLong(FILE_SIZE_AVG);
            pipeSize = jobParameters.getLong(PIPELINING);
        }
        long freeMemory = Runtime.getRuntime().freeMemory();
        long maxMemory = Runtime.getRuntime().maxMemory();
        long allocatedMemory = Runtime.getRuntime().totalMemory();
        long memory = allocatedMemory - freeMemory;
        for (DataInflux dataInflux : pmeterMetrics) {
            dataInflux.setConcurrency(threadPoolManager.concurrencyCount());
            dataInflux.setParallelism(threadPoolManager.parallelismCount());
            dataInflux.setPipelining((int) pipeSize); //if this is to be dynamic then we would need to adjust the clients.
            dataInflux.setThroughput(this.previousParentMetric.getThroughput());
            dataInflux.setDataBytesSent(this.previousParentMetric.getBytesSent());
            dataInflux.setFreeMemory(freeMemory);
            dataInflux.setMaxMemory(maxMemory);
            dataInflux.setAllocatedMemory(allocatedMemory);
            dataInflux.setMemory(memory);
            dataInflux.setJobSize(jobSize);
            dataInflux.setAvgFileSize(avgFileSize);
            dataInflux.setCompression(connectionBag.isCompression());
            dataInflux.setJobId(previousParentMetric.getJobId());
            dataInflux.setOdsUser(this.odsUser);
            dataInflux.setTransferNodeName(this.appName);
            log.info("Pushing DataInflux {}", dataInflux.toString());
        }
        influxIOService.insertDataPoints(pmeterMetrics);
        this.influxCache.clearCache();
    }

}
