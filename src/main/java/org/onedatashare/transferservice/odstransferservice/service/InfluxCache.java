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

    public enum ThroughputType {
        READER,
        WRITER
    }

    public InfluxCache() {
        this.threadCache = new ConcurrentHashMap<>();
    }

    public void addMetric(StepExecution stepExecution, long totalBytes, LocalDateTime startTime, LocalDateTime endTime, ThroughputType type) {
        if (!isJobMetricCollectionEnabled) {
            return;
        }
        JobMetric jobMetric = this.threadCache.getOrDefault(Thread.currentThread().getId(), new JobMetric());
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
        calculateThroughputAndUpdateCache(jobMetric, startTime, endTime, totalBytes, type);

        this.threadCache.put(Thread.currentThread().getId(), jobMetric);
    }

    private void calculateThroughputAndUpdateCache(JobMetric jobMetric, LocalDateTime startTime, LocalDateTime endTime, long currentDataBytes, ThroughputType type) {
        long timeItTookForThisList = Duration.between(startTime, endTime).toMillis() + jobMetric.getTotalTime();
        jobMetric.setTotalTime(timeItTookForThisList);

        currentDataBytes += jobMetric.getBytesSent();
        jobMetric.setBytesSent(currentDataBytes);

        double throughput = (double) jobMetric.getBytesSent() / jobMetric.getTotalTime();
        throughput = throughput * 1000;

        if(type == ThroughputType.READER) {
            jobMetric.setReadThroughput(throughput);
        } else {
            jobMetric.setWriteThroughput(throughput);
        }
    }

    public void addMetric(StepExecution stepExecution, long totalBytes, LocalDateTime startTime, LocalDateTime endTime) {
        addMetric(stepExecution, totalBytes, startTime, endTime, ThroughputType.WRITER);
    }

    public JobMetric someJobMetric() {
        DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
        long totalBytes = 0L;
        Iterator itr = this.threadCache.values().iterator();
        JobMetric jobMetric = new JobMetric();
        while (itr.hasNext()) {
            jobMetric = (JobMetric) itr.next();
            stats.accept(jobMetric.getWriteThroughput());
            totalBytes += jobMetric.getBytesSent();
        }
        jobMetric.setWriteThroughput(stats.getAverage());
        jobMetric.setBytesSent(totalBytes);
        return jobMetric;
    }

    public void clearCache() {
        this.threadCache.clear();
    }
}
