package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.OWNER_ID;

@Component
public class InfluxCache {

    public ConcurrentLinkedQueue<JobMetric> threadCache;

    @Autowired
    ThreadPoolManager threadPoolManager;

    public InfluxCache() {
        this.threadCache = new ConcurrentLinkedQueue<JobMetric>();
    }

    public void addMetric(double throughput, StepExecution stepExecution, int size) {
        JobMetric jobMetric = new JobMetric();
        jobMetric.setStepExecution(stepExecution);
        jobMetric.setPipelining(size);
        jobMetric.setConcurrency(threadPoolManager.concurrencyCount());
        jobMetric.setParallelism(threadPoolManager.parallelismCount());
        jobMetric.setOwnerId(stepExecution.getJobExecution().getJobParameters().getString(OWNER_ID));
        jobMetric.setJobId(String.valueOf(stepExecution.getJobExecutionId()));
        jobMetric.setThroughput(throughput);
        this.threadCache.add(jobMetric);
    }

    public void clearCache() {
        this.threadCache.clear();
    }
}
