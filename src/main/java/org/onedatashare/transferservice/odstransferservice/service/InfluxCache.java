package org.onedatashare.transferservice.odstransferservice.service;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentLinkedQueue;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.OWNER_ID;

@Component
public class InfluxCache {

    public ConcurrentLinkedQueue threadCache;

    @Autowired
    ThreadPoolManager threadPoolManager;

    public InfluxCache() {
        this.threadCache = new ConcurrentLinkedQueue();
    }

    public void addMetric(double throughput, StepExecution stepExecution, int size) {
        JobMetric jobMetric = new JobMetric();
        jobMetric.setThroughput(throughput);
        jobMetric.setStepExecution(stepExecution);
        jobMetric.setPipelining(size);
        jobMetric.setConcurrency(threadPoolManager.concurrencyCount());
        jobMetric.setParallelism(threadPoolManager.parallelismCount());
        jobMetric.setJobId(stepExecution.getJobExecutionId().toString());
        jobMetric.setOwnerId(stepExecution.getJobExecution().getJobParameters().getString(OWNER_ID));
        jobMetric.setJobId(String.valueOf(stepExecution.getJobExecutionId()));
        this.threadCache.add(jobMetric);
    }

    public void clearCache() {
        this.threadCache.clear();
    }
}
