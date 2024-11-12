package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.MetricsCollector;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterRead;
import org.springframework.batch.core.annotation.AfterWrite;
import org.springframework.batch.core.annotation.BeforeRead;
import org.springframework.batch.core.annotation.BeforeWrite;
import org.springframework.batch.item.Chunk;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;

public class ODSBaseWriter {

    protected StepExecution stepExecution;

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
        this.writeStartTimes.put(Thread.currentThread().threadId(), startWriteTime);
    }

    @AfterWrite
    public void afterWrite(Chunk<? extends DataChunk> chunk) {
        List<? extends DataChunk> items = chunk.getItems();
        LocalDateTime writeEndTime = LocalDateTime.now();
        long totalBytes = items.stream().mapToLong(DataChunk::getSize).sum();
        long threadId = Thread.currentThread().threadId();
        LocalDateTime writeStartTime = this.writeStartTimes.remove(threadId);
        //this is a cache for the optimizer directly in. This i actually think should be deleted and all data querying maybe ideally is done through the monitoring interface
        influxCache.addMetric(threadId, stepExecution, totalBytes, writeStartTime, writeEndTime, InfluxCache.ThroughputType.WRITER, items.get(0).getSize());

    }

    @BeforeRead
    public void beforeRead() {
        LocalDateTime startReadTime = LocalDateTime.now();
        this.readStartTimes.put(Thread.currentThread().threadId(), startReadTime);
    }

    @AfterRead
    public void afterRead(DataChunk item) {
        LocalDateTime endTime = LocalDateTime.now();
        long threadId = Thread.currentThread().threadId();
        if (item == null) {
            return;
        }
        LocalDateTime readStartTime = this.readStartTimes.remove(threadId);
        if (readStartTime == null) return;
        influxCache.addMetric(threadId, stepExecution, item.getSize(), readStartTime, endTime, InfluxCache.ThroughputType.READER, item.getSize());
    }
}
