package org.onedatashare.transferservice.odstransferservice.model.optimizer;

import lombok.Data;

@Data
public class OptimizerCreateRequest {
    String nodeId;
    int maxConcurrency;
    int maxParallelism;
    int maxPipelining;
    int maxChunkSize;

    String optimizerType;

    long fileCount;
    Long jobId;

    public OptimizerCreateRequest(String nodeId, int maxConcurrency, int maxParallelism, int maxPipelining, String optimizerType, long fileCount, long jobId) {
        this.maxConcurrency = maxConcurrency;
        this.maxChunkSize = Integer.MAX_VALUE;
        this.maxParallelism = maxParallelism;
        this.maxPipelining = maxPipelining;
        this.nodeId = nodeId;
        this.optimizerType = optimizerType;
        this.fileCount = fileCount;
        this.jobId = jobId;
    }
}
