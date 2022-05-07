package org.onedatashare.transferservice.odstransferservice.model.optimizer;

import lombok.Data;

@Data
public class Optimizer {
    int concurrency;
    int parallelism;
    int pipelining;
    long chunkSize;
}