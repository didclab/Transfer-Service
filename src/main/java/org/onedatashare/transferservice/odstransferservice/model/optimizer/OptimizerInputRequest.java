package org.onedatashare.transferservice.odstransferservice.model.optimizer;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;

@Data
public class OptimizerInputRequest {

    @Value("${spring.application.name}")
    String nodeId;
    double throughput;
    double rtt;
    int concurrency;
    int parallelism;
    int pipelining;
    long chunkSize;
}