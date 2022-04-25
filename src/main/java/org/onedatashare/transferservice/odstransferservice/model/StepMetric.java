package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

/**
 * @author deepika
 */
@Data
public class StepMetric {

    private Double throughput;
    private String fileId;
    private Integer chunkSize;
    private Long fileSize;
    private Double readerThroughput;
    private Double writerThroughput;
}
