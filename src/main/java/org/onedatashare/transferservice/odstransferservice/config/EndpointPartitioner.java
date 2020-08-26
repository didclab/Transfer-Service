package org.onedatashare.transferservice.odstransferservice.config;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.Map;

public class EndpointPartitioner implements Partitioner {
    @Override
    public Map<String, ExecutionContext> partition(int i) {
        return null;
    }
}
