package org.onedatashare.transferservice.odstransferservice.service.step;

import com.netflix.discovery.converters.Auto;
import lombok.Getter;
import lombok.Setter;
import org.apache.tomcat.jni.Local;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.MetricCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterRead;
import org.springframework.batch.core.annotation.AfterWrite;
import org.springframework.batch.core.annotation.BeforeRead;
import org.springframework.batch.core.annotation.BeforeWrite;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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

    MetricCache metricCache;

    public ODSBaseWriter(MetricsCollector metricsCollector, InfluxCache influxCache, MetricCache metricCache){
        this.readStartTimes = new HashMap<>();
        this.writeStartTimes = new HashMap<>();
        this.metricsCollector = metricsCollector;
        this.influxCache = influxCache;
        this.metricCache = metricCache;
    }

    @BeforeWrite
    public void beforeWrite() {
        LocalDateTime startWriteTime = LocalDateTime.now();
        logger.info("Before Write ThreadID: {}, put in time: {}", Thread.currentThread().getId(), startWriteTime);
        this.writeStartTimes.put(Thread.currentThread().getId(), startWriteTime);
    }

    @AfterWrite
    public void afterWrite(List<? extends DataChunk> items) {
        if(items == null) return;
        LocalDateTime writeEndTime = LocalDateTime.now();
        long totalBytes = items.stream().mapToLong(DataChunk::getSize).sum();
        LocalDateTime writeStartTime = this.writeStartTimes.get(Thread.currentThread().getId());
        double throughput = ODSConstants.computeThroughput(totalBytes, Duration.between(writeStartTime, writeEndTime));
        logger.info("ThreadId: {} writing hit throughput: {} Mbps {} bytes  {} startTime {} endTime", Thread.currentThread().getId(), throughput, totalBytes, writeStartTime, writeEndTime);
        metricCache.addMetric(Thread.currentThread().getName(), throughput, stepExecution, items.size());
        influxCache.addMetric(Thread.currentThread().getId(), stepExecution, totalBytes, writeStartTime, writeEndTime, InfluxCache.ThroughputType.WRITER);
    }

    @BeforeRead
    public void beforeRead() {
        LocalDateTime startReadTime = LocalDateTime.now();
        logger.info("Before Read ThreadID: {}, put in time: {}", Thread.currentThread().getId(), startReadTime);
        this.readStartTimes.put(Thread.currentThread().getId(), startReadTime);
    }

    @AfterRead
    public void afterRead(DataChunk item) {
        LocalDateTime endTime = LocalDateTime.now();
        if(item == null){return;}
        LocalDateTime readStartTime = this.readStartTimes.get(Thread.currentThread().getId());
        if(readStartTime == null) return;
        double throughput = ODSConstants.computeThroughput(item.getSize(), Duration.between(readStartTime, endTime));
        logger.info("ThreadId: {} reading hit throughput: {} Mbps {} bytes  {} seconds", Thread.currentThread().getId(), throughput, item.getSize(), Duration.between(readStartTime, endTime).toMillis()/1000);
        influxCache.addMetric(Thread.currentThread().getId(), stepExecution, item.getSize(), readStartTime, endTime, InfluxCache.ThroughputType.READER);
    }
}
