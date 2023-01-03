package org.onedatashare.transferservice.odstransferservice.service.cron;

import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.influx.InfluxMeterRegistry;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.DataRepository.InfluxIOService;
import org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants;
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

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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

    @Autowired
    InfluxMeterRegistry registry;

    private AtomicLong jobSize = new AtomicLong(0L);
    private AtomicLong avgFileSize = new AtomicLong(0L);
    private AtomicLong pipeSize = new AtomicLong(0L);

    private AtomicDouble throughput = new AtomicDouble(0L);
    private AtomicDouble readThroughput = new AtomicDouble(0L);
    private AtomicDouble writeThroughput = new AtomicDouble(0L);

    @PostConstruct
    public void postConstruct() {
        Metrics.gauge(DataInfluxConstants.AVERAGE_JOB_SIZE, avgFileSize);
        Metrics.gauge(DataInfluxConstants.PIPELINING, pipeSize);
        Metrics.gauge(DataInfluxConstants.JOB_SIZE, jobSize);
        Metrics.gauge(DataInfluxConstants.THROUGHPUT, throughput);

        Metrics.gauge(DataInfluxConstants.READ_THROUGHPUT, readThroughput);
        Metrics.gauge(DataInfluxConstants.WRITE_THROUGHPUT, writeThroughput);
    }

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

        String destType = "";
        String sourceType = "";
        String ownerId = this.odsUser;

        if (this.previousParentMetric.getStepExecution() != null) {
            JobParameters jobParameters = this.previousParentMetric.getStepExecution().getJobParameters();
            jobSize.set(jobParameters.getLong(JOB_SIZE));
            avgFileSize.set(jobParameters.getLong(FILE_SIZE_AVG));
            pipeSize.set(jobParameters.getLong(PIPELINING));
            throughput.set(this.previousParentMetric.getWriteThroughput());

            readThroughput.set(this.previousParentMetric.getReadThroughput());
            writeThroughput.set(this.previousParentMetric.getWriteThroughput());

            sourceType = jobParameters.getString(SOURCE_CREDENTIAL_TYPE);
            destType = jobParameters.getString(DEST_CREDENTIAL_TYPE);
            ownerId = jobParameters.getString(OWNER_ID);

            Iterable<Tag> tags = List.of((
                    Tag.of(DataInfluxConstants.SOURCE_TYPE, sourceType)),
                    Tag.of(DataInfluxConstants.DESTINATION_TYPE, destType),
                    Tag.of(DataInfluxConstants.ODS_USER, ownerId),
                    Tag.of(DataInfluxConstants.TRANSFER_NODE_NAME, this.appName)
            );
            Metrics.gauge(DataInfluxConstants.JOB_ID, tags, previousParentMetric.getJobId());
        }
        long freeMemory = Runtime.getRuntime().freeMemory();
        long maxMemory = Runtime.getRuntime().maxMemory();
        long allocatedMemory = Runtime.getRuntime().totalMemory();
        long memory = allocatedMemory - freeMemory;
        for (DataInflux dataInflux : pmeterMetrics) {
            dataInflux.setConcurrency(threadPoolManager.concurrencyCount()); // TODO Micrometer threadpool
            dataInflux.setParallelism(threadPoolManager.parallelismCount()); // TODO Micrometer threadpool
            dataInflux.setPipelining((int) pipeSize.get()); //if this is to be dynamic then we would need to adjust the clients.
            dataInflux.setThroughput(Math.min(this.previousParentMetric.getWriteThroughput(), this.previousParentMetric.getReadThroughput()));
            dataInflux.setDataBytesSent(this.previousParentMetric.getBytesSent());
            dataInflux.setFreeMemory(freeMemory); // TODO - Leave it
            dataInflux.setMaxMemory(maxMemory); // TODO - JVM properties
            dataInflux.setAllocatedMemory(allocatedMemory); // TODO - JVM properties
            dataInflux.setMemory(memory); // TODO - JVM properties
            dataInflux.setJobSize(jobSize.get());
            dataInflux.setAvgFileSize(avgFileSize.get());
            dataInflux.setCompression(connectionBag.isCompression());
            dataInflux.setJobId(previousParentMetric.getJobId());
            dataInflux.setOdsUser(ownerId);
            dataInflux.setTransferNodeName(this.appName);
            dataInflux.setSourceType(sourceType);
            dataInflux.setDestType(destType);
            log.info("Pushing DataInflux {}", dataInflux);
        }
        influxIOService.insertDataPoints(pmeterMetrics);
        this.influxCache.clearCache();
    }

}
