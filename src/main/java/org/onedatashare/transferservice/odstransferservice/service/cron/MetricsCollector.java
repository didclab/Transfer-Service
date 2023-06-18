package org.onedatashare.transferservice.odstransferservice.service.cron;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.InfluxIOService;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.LatencyRtt;
import org.onedatashare.transferservice.odstransferservice.service.PmeterParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;


@Service
@Getter
@Setter
public class MetricsCollector {

    private InfluxIOService influxIOService;
    private AtomicLong jobId;
    private Logger log = LoggerFactory.getLogger(MetricsCollector.class);

    @Value("${pmeter.cron.run}")
    private boolean isPmeterEnabled;

    @Value("${spring.application.name}")
    String appName;

    @Value("${ods.user}")
    String odsUser;

    InfluxCache influxCache;

    PmeterParser pmeterParser;

    private LatencyRtt latencyRtt;
    private List<DataInflux> metrics;

    public MetricsCollector(PmeterParser pmeterParser, InfluxCache influxCache, InfluxIOService influxIOService, LatencyRtt latencyRtt) {
        this.pmeterParser = pmeterParser;
        this.influxCache = influxCache;
        this.influxIOService = influxIOService;
        this.latencyRtt = latencyRtt;
        this.metrics = new ArrayList<>();
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
        if(this.isPmeterEnabled){
            pmeterParser.runPmeter();
            this.metrics = pmeterParser.parsePmeterOutput();
        }
        long freeMem = Runtime.getRuntime().freeMemory();
        long totalMem = Runtime.getRuntime().totalMemory();
        long usedMemory = totalMem - freeMem;
        long maxMem = Runtime.getRuntime().maxMemory();
        JobMetric currentAggregateMetric = influxCache.aggregateMetric(); //this metrics throughput is the throughput of the whole map in influxCache.
        DataInflux lastPmeterData;
        if(this.metrics.size() < 1){
            this.metrics.add(new DataInflux());
            lastPmeterData = metrics.get(metrics.size()-1);
        }else{
            lastPmeterData = metrics.get(metrics.size() - 1);
        }
        lastPmeterData.setAllocatedMemory(totalMem);
        lastPmeterData.setMemory(usedMemory);
        lastPmeterData.setFreeMemory(freeMem);
        lastPmeterData.setMaxMemory(maxMem);
        lastPmeterData.setCoreCount(Runtime.getRuntime().availableProcessors());
        lastPmeterData.setTransferNodeName(this.appName);
        if (currentAggregateMetric != null) {
            StepExecution stepExecution = currentAggregateMetric.getStepExecution();
            JobParameters jobParameters = stepExecution.getJobParameters();
            lastPmeterData.setOdsUser(jobParameters.getString(OWNER_ID));
            //Getting & setting source/destination RTT/
            double sourceRtt = 0.0;
            if(!jobParameters.getString(SOURCE_CREDENTIAL_TYPE).equals("vfs")){
                sourceRtt = this.latencyRtt.rttCompute(jobParameters.getString(SOURCE_HOST), jobParameters.getLong(SOURCE_PORT).intValue());
            }
            double destRtt = 0.0;
            if(!jobParameters.getString(DEST_CREDENTIAL_TYPE).equals("vfs")) {
                destRtt = this.latencyRtt.rttCompute(jobParameters.getString(DEST_HOST), jobParameters.getLong(DEST_PORT).intValue());
            }
            lastPmeterData.setSourceRtt(sourceRtt);
            lastPmeterData.setSourceLatency(sourceRtt/2);
            lastPmeterData.setDestinationRtt(destRtt);
            lastPmeterData.setDestLatency(destRtt/2);
            //application level parameters
            lastPmeterData.setConcurrency(currentAggregateMetric.getConcurrency());
            lastPmeterData.setParallelism(currentAggregateMetric.getParallelism());
            lastPmeterData.setPipelining(currentAggregateMetric.getPipelining());
            lastPmeterData.setChunksize(currentAggregateMetric.getChunkSize());
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
        }
        this.influxCache.clearCache();
        influxIOService.insertDataPoint(lastPmeterData);
    }
}
