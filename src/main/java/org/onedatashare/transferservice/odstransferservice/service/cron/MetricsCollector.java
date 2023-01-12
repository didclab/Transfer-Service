package org.onedatashare.transferservice.odstransferservice.service.cron;

import com.influxdb.client.write.Point;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.InfluxIOService;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.PmeterParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


@Service
@Getter
@Setter
public class MetricsCollector {

    private InfluxIOService influxIOService;
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

    JobExplorer jobExplorer;

    public MetricsCollector(PmeterParser pmeterParser, InfluxCache influxCache, JobExplorer jobExplorer, InfluxIOService influxIOService) {
        this.pmeterParser = pmeterParser;
        this.influxCache = influxCache;
        this.jobExplorer = jobExplorer;
        this.influxIOService = influxIOService;
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
        long freeMem = Runtime.getRuntime().freeMemory();
        long totalMem = Runtime.getRuntime().totalMemory();
        long usedMemory = totalMem - freeMem;
        long maxMem = Runtime.getRuntime().maxMemory();
        JobMetric currentAggregateMetric = influxCache.aggregateMetric(); //this metrics throughput is the throughput of the whole map in influxCache.
        DataInflux lastPmeterData = pmeterMetrics.get(pmeterMetrics.size()-1);
        lastPmeterData.setAllocatedMemory(totalMem);
        lastPmeterData.setMemory(usedMemory);
        lastPmeterData.setFreeMemory(freeMem);
        lastPmeterData.setMaxMemory(maxMem);
        lastPmeterData.setCoreCount(Runtime.getRuntime().availableProcessors());
        lastPmeterData.setOdsUser(this.odsUser);
        lastPmeterData.setTransferNodeName(this.appName);
        if (currentAggregateMetric != null) {
            StepExecution stepExecution = currentAggregateMetric.getStepExecution();
            JobParameters jobParameters = stepExecution.getJobParameters();
            lastPmeterData.setConcurrency(currentAggregateMetric.getConcurrency());
            lastPmeterData.setParallelism(currentAggregateMetric.getParallelism());
            lastPmeterData.setPipelining(currentAggregateMetric.getPipelining());
            //JobMetric stuff
            lastPmeterData.setReadThroughput(currentAggregateMetric.getReadThroughput());
            lastPmeterData.setWriteThroughput(currentAggregateMetric.getWriteThroughput());
            lastPmeterData.setBytesRead(currentAggregateMetric.getReadBytes());
            lastPmeterData.setBytesWritten(currentAggregateMetric.getWrittenBytes());
            lastPmeterData.setDestType(jobParameters.getString(ODSConstants.DEST_CREDENTIAL_TYPE));
            lastPmeterData.setDestCredId(jobParameters.getString(ODSConstants.DEST_CREDENTIAL_ID));
            lastPmeterData.setSourceType(jobParameters.getString(ODSConstants.SOURCE_CREDENTIAL_TYPE));
            lastPmeterData.setSourceCredId(jobParameters.getString(ODSConstants.SOURCE_CREDENTIAL_ID));

            lastPmeterData.setJobId(stepExecution.getJobExecutionId());
            lastPmeterData.setJobSize(jobParameters.getLong(ODSConstants.JOB_SIZE));
            lastPmeterData.setAvgFileSize(jobParameters.getLong(ODSConstants.FILE_SIZE_AVG));
            lastPmeterData.setOdsUser(jobParameters.getString(ODSConstants.OWNER_ID));
            this.influxCache.clearCache();
        }
        influxIOService.insertDataPoint(lastPmeterData);
    }
}
