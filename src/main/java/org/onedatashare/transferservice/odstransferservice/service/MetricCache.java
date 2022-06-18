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
            Metric metric = threadCache.get(threadId);
            if(metric == null){
                this.threadCache.put(threadId, new Metric(throughput, stepExecution, pipeSize));
            }else{
                this.threadCache.put(threadId, new Metric((throughput + metric.getThroughput())/2, stepExecution, pipeSize));
            }
        }
    }

    public void clearCache() {
        if (this.threadCache != null && this.optimizerEnable) {
            this.threadCache.clear();
        }
    }
}
