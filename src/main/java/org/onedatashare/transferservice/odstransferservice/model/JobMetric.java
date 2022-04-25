package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

import java.util.List;

/**
 * @author deepika
 */
@Data
public class JobMetric {
    private String jobId;
    private Double throughput;
    private String ownerId;
    private Long concurrency;
    private Long parallelism;
    private Long pipelining;
    private Double readerThroughput;
    private Double writerThroughput;
    private List<StepMetric> stepMetricList;
}
