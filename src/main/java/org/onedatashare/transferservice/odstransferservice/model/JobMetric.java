package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.springframework.batch.core.StepExecution;

import java.time.LocalDateTime;

/**
 * @author deepika
 */
@Data
public class JobMetric {
    private long threadId; //threadId this jobMetric is associated too
    private StepExecution stepExecution; //associated stepExecution of this jobMetric
    private Integer concurrency; //the concurrency pool at time of reading or writing
    private Integer parallelism; //The parallelism pool size at time of reading or writing
    private Integer pipelining; //pipeSize used.
    private LocalDateTime writeStartTime;
    private LocalDateTime writeEndTime;
    private Double writeThroughput; //the write out throughput
    private long writtenBytes; //number of bytes written out
    private LocalDateTime readStartTime;
    private LocalDateTime readEndTime;
    private long readBytes; //number of bytes read in
    private Double readThroughput; //the read in throughput

    public JobMetric() {
        this.threadId = -1;
        this.concurrency = -1;
        this.parallelism = -1;
        this.pipelining = -1;
        this.writeThroughput = 0.0;
        this.readThroughput = 0.0;
        this.stepExecution = null;
        this.writtenBytes = 0L;
        this.readBytes = 0L;
    }
    public boolean isDefault() {
        return this.stepExecution != null;
    }

}
