package org.onedatashare.transferservice.odstransferservice.model.optimizer;

import lombok.Data;

@Data
public class OptimizerDeleteRequest {
    private String nodeId;

    public OptimizerDeleteRequest(String nodeId) {
        this.nodeId = nodeId;
    }
}
