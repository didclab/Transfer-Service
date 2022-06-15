package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.Metric;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class MetricCache {

    ConcurrentHashMap<String, Metric> threadCache;

    @Value("${optimizer.enable}")
    private boolean optimizerEnable;

    public MetricCache() {
        this.threadCache = new ConcurrentHashMap<>();
    }

    public void addMetric(String threadId, double throughput, StepExecution stepExecution, int pipeSize) {
        if (this.optimizerEnable) {
            this.threadCache.put(threadId, new Metric(throughput, stepExecution, pipeSize));
        }
    }

    public void clearCache() {
        if (this.threadCache != null && this.optimizerEnable) {
            this.threadCache.clear();
        }
    }
}
