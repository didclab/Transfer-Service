package org.onedatashare.transferservice.odstransferservice.model.optimizer;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

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
    String dbType;
    String jobUuid;
    String userId;

    public OptimizerCreateRequest(String userId,String nodeId, int maxConcurrency, int maxParallelism, int maxPipelining, String optimizerType, long fileCount, long jobId, String dbType, String jobUuid) {
        this.userId = userId;
        this.maxConcurrency = maxConcurrency;
        this.maxChunkSize = Integer.MAX_VALUE;
        this.maxParallelism = maxParallelism;
        this.maxPipelining = maxPipelining;
        this.nodeId = nodeId;
        this.optimizerType = optimizerType;
        this.fileCount = fileCount;
        this.jobId = jobId;
        this.dbType = dbType;
        this.jobUuid = jobUuid;
    }
}
