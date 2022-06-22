package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.DoubleSummaryStatistics;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.OWNER_ID;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.PIPELINING;

@Component
public class InfluxCache {

    public ConcurrentHashMap<Long, JobMetric> threadCache;

    @Value("${job.metrics.save}")
    private boolean isJobMetricCollectionEnabled;

    @Autowired
    ThreadPoolManager threadPoolManager;

    public InfluxCache() {
        this.threadCache = new ConcurrentHashMap<>();
    }

    public void addMetric(StepExecution stepExecution, int size, long totalBytes, LocalDateTime readStartTime, LocalDateTime writeEndTime) {
        if (!isJobMetricCollectionEnabled) {
            return;
        }
        JobMetric jobMetric = this.threadCache.get(Thread.currentThread().getId());
        if (jobMetric == null) {
            jobMetric = new JobMetric();
        }
        totalBytes += jobMetric.getBytesSent();
        long timeItTookForThisList = Duration.between(readStartTime, writeEndTime).toMillis() + jobMetric.getTotalTime();
        jobMetric.setStepExecution(stepExecution);
        jobMetric.setConcurrency(threadPoolManager.concurrencyCount());
        jobMetric.setParallelism(threadPoolManager.parallelismCount());
        try{
            jobMetric.setPipelining(Math.toIntExact(stepExecution.getJobParameters().getLong(PIPELINING)));
        }catch (NullPointerException ignored){
            jobMetric.setPipelining(0);
        }

        jobMetric.setOwnerId(stepExecution.getJobExecution().getJobParameters().getString(OWNER_ID));
        jobMetric.setJobId(stepExecution.getJobExecutionId());
        jobMetric.setThreadId(Thread.currentThread().getId());
        jobMetric.setBytesSent(totalBytes);
        jobMetric.setTotalTime(timeItTookForThisList);
        double throughput = (double) jobMetric.getBytesSent() / jobMetric.getTotalTime();
        throughput = throughput * 1000;
        jobMetric.setThroughput(throughput);
        this.threadCache.put(Thread.currentThread().getId(), jobMetric);
    }

    public JobMetric someJobMetric() {
        DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
        long totalBytes = 0L;
        Iterator itr = this.threadCache.values().iterator();
        JobMetric jobMetric = new JobMetric();
        while (itr.hasNext()) {
            jobMetric = (JobMetric) itr.next();
            stats.accept(jobMetric.getThroughput());
            totalBytes += jobMetric.getBytesSent();
        }
        jobMetric.setThroughput(stats.getAverage());
        jobMetric.setBytesSent(totalBytes);
        return jobMetric;
    }

    public void clearCache() {
        this.threadCache.clear();
    }
}
