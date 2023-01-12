package org.onedatashare.transferservice.odstransferservice.service.cron;

import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.influx.InfluxMeterRegistry;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.PmeterParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;


@Service
@Getter
@Setter
public class MetricsCollector {

    private AtomicLong jobId;
    private Logger log = LoggerFactory.getLogger(MetricsCollector.class);

    @Value("${pmeter.cron.run}")
    private boolean isCronEnabled;

    @Value("${spring.application.name}")
    String appName;

    @Value("${ods.user}")
    String odsUser;

    InfluxCache influxCache;

    PmeterParser pmeterParser;

    InfluxMeterRegistry influxMeterRegistry;

    JobExplorer jobExplorer;

    private AtomicLong memory;
    private AtomicLong maxMemory;
    private AtomicLong freeMemory;
    private AtomicLong totalBytesSent;
    private AtomicLong jobSize;
    private AtomicLong avgFileSize;
    private AtomicLong pipeSize;
    private AtomicDouble throughput;
    private AtomicDouble readThroughput;
    private AtomicDouble writeThroughput;
    private AtomicLong bytesDownloaded;
    private AtomicLong bytesUploaded;
//    private Iterable<Tag> tags;

    public MetricsCollector(InfluxMeterRegistry influxMeterRegistry, PmeterParser pmeterParser, InfluxCache influxCache, JobExplorer jobExplorer) {
        this.totalBytesSent = new AtomicLong();
        this.jobSize = new AtomicLong();
        this.avgFileSize = new AtomicLong();
        this.pipeSize = new AtomicLong();
        this.throughput = new AtomicDouble();
        this.readThroughput = new AtomicDouble();
        this.writeThroughput = new AtomicDouble();
        this.bytesDownloaded = new AtomicLong();
        this.bytesUploaded = new AtomicLong();
        this.influxMeterRegistry = influxMeterRegistry;
        this.pmeterParser = pmeterParser;
        this.influxCache = influxCache;
        this.jobExplorer = jobExplorer;
        this.memory = new AtomicLong();
        this.maxMemory = new AtomicLong();
        this.freeMemory = new AtomicLong();
        this.jobId = new AtomicLong();
//        this.tags = List.of(Tag.of(DataInfluxConstants.TRANSFER_NODE_NAME, this.appName));
        log.info("InfluxCache has size of: {}", this.influxCache.threadCache.size());
    }

    @PostConstruct
    public void postConstruct() {
        Gauge.builder(DataInfluxConstants.READ_THROUGHPUT, readThroughput, AtomicDouble::doubleValue)
                .description("The read throughput")
                .baseUnit("bytes")
                .register(influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.WRITE_THROUGHPUT, writeThroughput, AtomicDouble::doubleValue)
                .description("The write throughput")
                .baseUnit("bytes")
                .register(influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.THROUGHPUT, throughput, AtomicDouble::doubleValue)
                .description("The overall throughput")
                .baseUnit("bytes")
                .register(influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.AVERAGE_JOB_SIZE, avgFileSize, AtomicLong::longValue)
                .description("Average file size of the job")
                .baseUnit("bytes")
                .register(influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.JOB_SIZE, jobSize, AtomicLong::longValue)
                .description("Job size in bytes")
                .baseUnit("bytes")
                .register(influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.PIPELINING, pipeSize, AtomicLong::longValue)
                .description("Average Pipelining for throughput")
                .baseUnit("threads")
                .register(influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.BYTES_DOWNLOADED, this.bytesDownloaded, AtomicLong::longValue)
                .description("Bytes Downloaded since last time segment")
                .baseUnit("bytes")
                .register(influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.BYTES_UPLOADED, this.bytesUploaded, AtomicLong::longValue)
                .description("Bytes Uploaded since last time segment")
                .baseUnit("bytes")
                .register(influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.MEMORY, this.memory, AtomicLong::longValue)
                .description("Memory used by application.")
                .register(influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.MAX_MEMORY, this.maxMemory, AtomicLong::longValue)
                .description("Maximum memory to be used by JVM")
                .register(influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.FREE_MEMORY, this.freeMemory, AtomicLong::longValue)
                .description("The memory available to be consumed")
                .register(influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.JOB_ID, this.jobId, AtomicLong::longValue)
                .description("The JobId associated with this dimension")
                .register(influxMeterRegistry);
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
        JobMetric currentAggregateMetric = influxCache.aggregateMetric(); //this metrics throughput is the throughput of the whole map in influxCache.
        if (currentAggregateMetric != null) {
            JobParameters jobParameters = currentAggregateMetric.getStepExecution().getJobParameters();
            jobSize.set(jobParameters.getLong(JOB_SIZE));
            avgFileSize.set(jobParameters.getLong(FILE_SIZE_AVG));
            pipeSize.set(currentAggregateMetric.getPipelining());
            readThroughput.set(currentAggregateMetric.getReadThroughput());
            writeThroughput.set(currentAggregateMetric.getWriteThroughput());
            throughput.set(Math.min(readThroughput.doubleValue(), writeThroughput.doubleValue()));
            bytesDownloaded.set(currentAggregateMetric.getReadBytes());
            bytesUploaded.set(currentAggregateMetric.getWrittenBytes());
//            this.tags = List.of((
//                            Tag.of(DataInfluxConstants.SOURCE_TYPE, jobParameters.getString(SOURCE_CREDENTIAL_TYPE))),
//                    Tag.of(DataInfluxConstants.DESTINATION_TYPE, jobParameters.getString(DEST_CREDENTIAL_TYPE)),
//                    Tag.of(DataInfluxConstants.ODS_USER, jobParameters.getString(OWNER_ID)),
//                    Tag.of(DataInfluxConstants.TRANSFER_NODE_NAME, this.appName),
//                    Tag.of(DataInfluxConstants.JOB_ID, String.valueOf(currentAggregateMetric.getStepExecution().getJobExecutionId())));
            jobId.set(currentAggregateMetric.getStepExecution().getJobExecutionId());
            this.influxCache.clearCache();
        }else{
            readThroughput.set(0);
            writeThroughput.set(0);
            throughput.set(0);
            bytesDownloaded.set(0);
            bytesUploaded.set(0);
        }
        this.memory.set(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
        this.maxMemory.set(Runtime.getRuntime().maxMemory());
        this.freeMemory.set(Runtime.getRuntime().freeMemory());
    }
}
