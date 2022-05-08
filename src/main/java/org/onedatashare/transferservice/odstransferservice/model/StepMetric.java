package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import org.springframework.batch.core.StepExecution;

/**
 * @author deepika
 */
@Data
public class StepMetric {

    private Double throughput;
    private Integer chunkSize;
    private Long fileSize;
    private Double readerThroughput;
    private Double writerThroughput;
    private StepExecution stepExecution;
}
