package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterRead;
import org.springframework.batch.core.annotation.AfterWrite;
import org.springframework.batch.core.annotation.BeforeRead;
import org.springframework.batch.core.annotation.BeforeWrite;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;

public class ODSBaseWriter {

    protected StepExecution stepExecution;

    Logger logger = LoggerFactory.getLogger(ODSBaseWriter.class);

    HashMap<Long, LocalDateTime> readStartTimes;
    HashMap<Long, LocalDateTime> writeStartTimes;
    MetricsCollector metricsCollector;

    InfluxCache influxCache;


    public ODSBaseWriter(MetricsCollector metricsCollector, InfluxCache influxCache) {
        this.readStartTimes = new HashMap<>();
        this.writeStartTimes = new HashMap<>();
        this.metricsCollector = metricsCollector;
        this.influxCache = influxCache;
    }

    @BeforeWrite
    public void beforeWrite() {
        LocalDateTime startWriteTime = LocalDateTime.now();
        this.writeStartTimes.put(Thread.currentThread().getId(), startWriteTime);
    }

    @AfterWrite
    public void afterWrite(List<? extends DataChunk> items) {
        if (items == null) return;
        LocalDateTime writeEndTime = LocalDateTime.now();
        long totalBytes = items.stream().mapToLong(DataChunk::getSize).sum();
        LocalDateTime writeStartTime = this.writeStartTimes.get(Thread.currentThread().getId());
        double throughput = ODSConstants.computeThroughput(totalBytes, Duration.between(writeStartTime, writeEndTime));
        logger.info("ThreadId: {} writing hit throughput: {} Mbps {} bytes  {} startTime {} endTime", Thread.currentThread().getId(), throughput, totalBytes, writeStartTime, writeEndTime);
        //this is a cache for the optimizer directly in. This i actually think should be deleted and all data querying maybe ideally is done through the monitoring interface
        influxCache.addMetric(Thread.currentThread().getId(), stepExecution, totalBytes, writeStartTime, writeEndTime, InfluxCache.ThroughputType.WRITER);
    }

    @BeforeRead
    public void beforeRead() {
        LocalDateTime startReadTime = LocalDateTime.now();
        this.readStartTimes.put(Thread.currentThread().getId(), startReadTime);
    }

    @AfterRead
    public void afterRead(DataChunk item) {
        LocalDateTime endTime = LocalDateTime.now();
        if (item == null) {
            return;
        }
        LocalDateTime readStartTime = this.readStartTimes.get(Thread.currentThread().getId());
        if (readStartTime == null) return;
        double throughput = ODSConstants.computeThroughput(item.getSize(), Duration.between(readStartTime, endTime));
        logger.info("ThreadId: {} reading hit throughput: {} Mbps {} bytes  {} seconds", Thread.currentThread().getId(), throughput, item.getSize(), Duration.between(readStartTime, endTime).toMillis() / 1000);
        influxCache.addMetric(Thread.currentThread().getId(), stepExecution, item.getSize(), readStartTime, endTime, InfluxCache.ThroughputType.READER);
    }
}
