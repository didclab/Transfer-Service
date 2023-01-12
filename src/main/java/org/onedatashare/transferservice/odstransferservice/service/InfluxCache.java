package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.DoubleSummaryStatistics;
import java.util.concurrent.ConcurrentHashMap;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.PIPELINING;

/**
 * InfluxCache is a class that stores the metadata/metric data on the threads that are sending the file.
 * The actual cache is a thread safe cache that should only ever hold one JobMetric that represents everything the thread has done.
 * So the idea is every add operation checks if the thread has a previous jobMetric. If so then it does an in-place update.
 * - addMetric() we do in-place bc doing parallel adds. Say to a list per thread would swell memory I believe.
 * - aggregateMetric() returns the single object that represents what all of the entires would represent. So avg read throughput for all jobmetrics in the map. Same thing for the write throughput and other properties.
 * StepExecution is the weird one, this should be the same for all threads actually so long as they are processing on the same file.
 * The cache gets cleared everytime the Metrics CRON runs.
 */
@Service
public class InfluxCache {

    public ConcurrentHashMap<Long, JobMetric> threadCache; //stores a JobMetric that represents everything that thread has processed for the step. Thus each JobMetric is an aggregate of what has happened

    @Value("${job.metrics.save}")
    private boolean isJobMetricCollectionEnabled;

    ThreadPoolManager threadPoolManager;

    Logger logger = LoggerFactory.getLogger(InfluxCache.class);

    public enum ThroughputType {
        READER,
        WRITER
    }

    /**
     * @param threadPoolManager - This is constructor dependency injection in spring for those of you reading.
     */
    public InfluxCache(ThreadPoolManager threadPoolManager) {
        this.threadPoolManager = threadPoolManager;
        this.threadCache = new ConcurrentHashMap<>();
    }

    public void addMetric(long threadId, StepExecution stepExecution, long totalBytes, LocalDateTime startTime, LocalDateTime endTime, ThroughputType type) {
        JobMetric prevMetric = this.threadCache.get(threadId);
        if (prevMetric == null) {
            prevMetric = new JobMetric();
            prevMetric.setThreadId(threadId);
            prevMetric.setStepExecution(stepExecution);
            prevMetric.setConcurrency(threadPoolManager.concurrencyCount());
            prevMetric.setParallelism(threadPoolManager.parallelismCount());
            prevMetric.setPipelining(stepExecution.getJobParameters().getLong(PIPELINING).intValue());
            this.threadCache.put(threadId, prevMetric);
        }
        if (type == ThroughputType.READER) {
            if (prevMetric.getReadStartTime() == null) {
                prevMetric.setReadStartTime(startTime);
            }
            prevMetric.setReadEndTime(endTime);
            Duration totalTime = Duration.between(prevMetric.getReadStartTime(), prevMetric.getReadEndTime());
            long rtb = totalBytes + prevMetric.getReadBytes();
            prevMetric.setReadBytes(rtb);
            prevMetric.setReadThroughput(ODSConstants.computeThroughput(rtb, totalTime));
        } else if (type == ThroughputType.WRITER) {
            if (prevMetric.getWriteStartTime() == null) {
                prevMetric.setWriteStartTime(startTime);
            }
            prevMetric.setWriteEndTime(endTime);
            Duration totalTime = Duration.between(prevMetric.getWriteStartTime(), prevMetric.getWriteEndTime());
            long wtb = totalBytes + prevMetric.getWrittenBytes();
            prevMetric.setWrittenBytes(wtb);
            prevMetric.setWriteThroughput(ODSConstants.computeThroughput(wtb, totalTime));
        }
    }

    /**
     * Every Thread adds to this cache in parallel, each entry contains what 1 thread achomplished.
     *
     * @return An Aggregate JobMetric which is the average or sum depending on the property of all the values in the pool
     */
    public JobMetric aggregateMetric() {
        if(this.threadCache.size() < 1) return null;

        JobMetric agg = new JobMetric();
        //need to find earliest start and latest late time for both read and write.
        for(JobMetric value : this.threadCache.values()){
            long readTotalBytes = agg.getReadBytes() + value.getReadBytes();
            agg.setReadBytes(readTotalBytes);
            long writeTotalBytes = agg.getWrittenBytes() + value.getWrittenBytes();
            agg.setWrittenBytes(writeTotalBytes);

            agg.setStepExecution(value.getStepExecution());
            agg.setConcurrency(value.getConcurrency());
            agg.setParallelism(value.getParallelism());
            agg.setPipelining(value.getPipelining());

            LocalDateTime valueReadStartTime = value.getReadStartTime();
            LocalDateTime aggReadStartTime = agg.getReadStartTime();
            //readStartTime gets set by the earliest readStartTime that is not null.
            if(aggReadStartTime == null && valueReadStartTime != null){
                agg.setReadStartTime(valueReadStartTime);
            }else if(aggReadStartTime != null && valueReadStartTime != null) {
                if(valueReadStartTime.isBefore(aggReadStartTime)){
                    agg.setReadStartTime(valueReadStartTime);
                }
            }
            //readEndTime
            LocalDateTime valueReadEndTime = value.getReadEndTime();
            LocalDateTime aggReadEndTime = agg.getReadEndTime();
            if(aggReadEndTime == null && valueReadEndTime != null){
                agg.setReadEndTime(valueReadEndTime);
            }else if(aggReadEndTime != null && valueReadEndTime != null){
                if(valueReadEndTime.isAfter(aggReadEndTime)){
                    agg.setReadEndTime(valueReadEndTime);
                }
            }
            //Write Start Time comparing
            LocalDateTime valueWriteStartTime = value.getWriteStartTime();
            LocalDateTime aggWriteStartTime = agg.getWriteStartTime();
            if(aggWriteStartTime == null && valueWriteStartTime != null){
                agg.setWriteStartTime(valueWriteStartTime);
            }else if(aggWriteStartTime != null && valueWriteStartTime != null) {
                if(valueWriteStartTime.isBefore(aggWriteStartTime)){
                    agg.setWriteStartTime(valueWriteStartTime);
                }
            }
            LocalDateTime valueWriteEndTime = value.getWriteEndTime();
            LocalDateTime aggWriteEndTime = agg.getWriteEndTime();
            if(aggWriteEndTime == null && valueWriteEndTime != null){
                agg.setWriteEndTime(valueWriteEndTime);
            }else if(aggWriteEndTime != null && valueWriteEndTime != null) {
                if(valueWriteEndTime.isAfter(aggWriteEndTime)){
                    agg.setWriteEndTime(valueWriteEndTime);
                }
            }

        }
        if(agg.getReadStartTime() != null && agg.getReadEndTime() != null){
            double readThroughput = ODSConstants.computeThroughput(agg.getReadBytes(), Duration.between(agg.getReadStartTime(), agg.getReadEndTime()));
            agg.setReadThroughput(readThroughput);
        }
        if(agg.getWriteStartTime() != null && agg.getWriteEndTime() != null ){
            double writeThroughput = ODSConstants.computeThroughput(agg.getWrittenBytes(), Duration.between(agg.getWriteStartTime(), agg.getWriteEndTime()));
            agg.setWriteThroughput(writeThroughput);
        }
        logger.info("Aggregate Metric created: {}", agg);
        return agg;
    }

    public void clearCache() {
        this.threadCache.clear();
        logger.info("ThreadCache cleared: current size: {}", this.threadCache.size());
    }
}
