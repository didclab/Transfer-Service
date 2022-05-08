package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.Metric;
import org.springframework.batch.core.StepExecution;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class MetricCache {

    ConcurrentHashMap<String, Metric> threadCache;

    public MetricCache() {
        this.threadCache = new ConcurrentHashMap<>();
    }

    public void addMetric(String threadId, double throughput, StepExecution stepExecution, int pipeSize) {
        this.threadCache.put(threadId, new Metric(throughput, stepExecution, pipeSize));
    }

    public void clearCache(){
        if(this.threadCache != null){
            this.threadCache.clear();
        }
    }
}
