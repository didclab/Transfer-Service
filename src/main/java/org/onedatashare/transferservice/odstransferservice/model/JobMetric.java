package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import org.springframework.batch.core.StepExecution;

/**
 * @author deepika
 */
@Data
public class JobMetric {
    private int cpus;
    private long memory;
    private String jobId;
    private Double throughput;
    private String ownerId;
    private Integer concurrency;
    private Integer parallelism;
    private Integer pipelining;
    private StepExecution stepExecution;

    public JobMetric() {
        this.cpus = Runtime.getRuntime().availableProcessors();
        this.memory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        this.jobId = "-1";
        this.throughput = 0.0;
        this.ownerId = "";
        this.concurrency = 0;
        this.parallelism = 0;
        this.pipelining = 0;
        this.stepExecution = null;
    }
}
