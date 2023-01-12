package org.onedatashare.transferservice.odstransferservice.service;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.springframework.batch.core.StepExecution;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;

@ExtendWith(MockitoExtension.class)
public class InfluxCacheTest {

    InfluxCache testObj;

    @Mock
    ThreadPoolManager mockedThreadPoolManager;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    StepExecution mockedStepExecution;

    @BeforeEach
    public void init() {
        testObj = new InfluxCache(mockedThreadPoolManager);
    }


    @Test
    public void testObjectCreates() {
        Assertions.assertEquals(0, testObj.threadCache.size());
    }

    public JobMetric buildReadExpectedMetric(LocalDateTime startTime, LocalDateTime endTime, long threadId, long totalBytes, int cc, int pp, int p) {
        double throughput = ODSConstants.computeThroughput(totalBytes, Duration.between(startTime, endTime));
        JobMetric expectedMetric = new JobMetric();
        expectedMetric.setThreadId(threadId);
        expectedMetric.setReadThroughput(throughput);
        expectedMetric.setReadBytes(totalBytes);
        expectedMetric.setReadStartTime(startTime);
        expectedMetric.setReadEndTime(endTime);
        expectedMetric.setConcurrency(cc);
        expectedMetric.setParallelism(p);
        expectedMetric.setPipelining(pp);
        expectedMetric.setStepExecution(this.mockedStepExecution);
        return expectedMetric;
    }

    public JobMetric buildWriteExpectedMetrics(LocalDateTime startTime, LocalDateTime endTime, long threadId, long totalBytes, int cc, int pp, int p) {
        double throughput = ODSConstants.computeThroughput(totalBytes, Duration.between(startTime, endTime));
        JobMetric expectedMetric = new JobMetric();
        expectedMetric.setThreadId(threadId);
        expectedMetric.setWriteThroughput(throughput);
        expectedMetric.setWrittenBytes(totalBytes);
        expectedMetric.setWriteStartTime(startTime);
        expectedMetric.setWriteEndTime(endTime);
        expectedMetric.setConcurrency(cc);
        expectedMetric.setParallelism(p);
        expectedMetric.setPipelining(pp);
        expectedMetric.setStepExecution(this.mockedStepExecution);
        return expectedMetric;
    }

    @Test
    public void testCacheOneSizeAndEnsureIsRead() {
        Mockito.when(mockedThreadPoolManager.concurrencyCount()).thenReturn(1);
        Mockito.when(mockedThreadPoolManager.parallelismCount()).thenReturn(1);
        Mockito.when(mockedStepExecution.getJobParameters().getLong(ODSConstants.PIPELINING)).thenReturn(1L);
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        long testThreadId = 1, totalBytes = 1;
        int cc = 1, pp = 1, p = 1;

        InfluxCache.ThroughputType type = InfluxCache.ThroughputType.READER;
        JobMetric expectedMetric = this.buildReadExpectedMetric(startTime, endTime, testThreadId, totalBytes, cc, pp, p);

        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes, startTime, endTime, type);
        Assertions.assertEquals(1, testObj.threadCache.size());

        //now test the quality of that 1 object
        JobMetric metric = testObj.threadCache.get(Thread.currentThread().getId());
        Assertions.assertEquals(expectedMetric, metric);
    }

    @Test
    public void testCacheOneSizeEnsureMetricIsWrite() {
        Mockito.when(mockedThreadPoolManager.concurrencyCount()).thenReturn(1);
        Mockito.when(mockedThreadPoolManager.parallelismCount()).thenReturn(1);
        Mockito.when(mockedStepExecution.getJobParameters().getLong(ODSConstants.PIPELINING)).thenReturn(1L);
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        long testThreadId = 1, totalBytes = 1;
        int cc = 1, pp = 1, p = 1;
        InfluxCache.ThroughputType type = InfluxCache.ThroughputType.WRITER;
        JobMetric expectedMetric = buildWriteExpectedMetrics(startTime, endTime, testThreadId, totalBytes, cc, pp, p);

        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes, startTime, endTime, type);
        Assertions.assertEquals(1, testObj.threadCache.size());

        //now test the quality of that 1 object
        JobMetric metric = testObj.threadCache.get(Thread.currentThread().getId());
        Assertions.assertEquals(expectedMetric, metric);
    }

    @Test
    public void testCacheTwoAddsOnOneThreadReadAndReadMetrics() {
        Mockito.when(mockedThreadPoolManager.concurrencyCount()).thenReturn(1);
        Mockito.when(mockedThreadPoolManager.parallelismCount()).thenReturn(1);
        Mockito.when(mockedStepExecution.getJobParameters().getLong(ODSConstants.PIPELINING)).thenReturn(1L);
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        long testThreadId = Thread.currentThread().getId(), totalBytes = 100;
        int cc = 1, pp = 1, p = 1;
        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes, startTime, endTime, InfluxCache.ThroughputType.READER);
        LocalDateTime startTime2 = LocalDateTime.now();
        LocalDateTime endTime2 = LocalDateTime.now();

        long totalBytes2 = 1000;
        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes2, startTime2, endTime2, InfluxCache.ThroughputType.READER);
        Assertions.assertEquals(1, testObj.threadCache.size()); //there is only 1 thread per entry

        //now test the quality of the one read object
        JobMetric metric = testObj.threadCache.get(testThreadId);
        Assertions.assertEquals(metric.getReadStartTime(), startTime);
        Assertions.assertEquals(metric.getReadEndTime(), endTime2);
        Assertions.assertEquals(metric.getReadBytes(), totalBytes+totalBytes2);
        Assertions.assertEquals(metric.getReadThroughput(), ODSConstants.computeThroughput(totalBytes+totalBytes2, Duration.between(startTime, endTime2)));
        Assertions.assertEquals(metric.getConcurrency(), 1);
        Assertions.assertEquals(metric.getParallelism(), 1);
        Assertions.assertEquals(metric.getPipelining(), 1);
        Assertions.assertEquals(metric.getStepExecution(), mockedStepExecution);
    }

    @Test
    public void testAddTwoWriteMetricsOneThread(){
        Mockito.when(mockedThreadPoolManager.concurrencyCount()).thenReturn(1);
        Mockito.when(mockedThreadPoolManager.parallelismCount()).thenReturn(1);
        Mockito.when(mockedStepExecution.getJobParameters().getLong(ODSConstants.PIPELINING)).thenReturn(1L);

        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        long testThreadId = Thread.currentThread().getId(), totalBytes = 100;
        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes, startTime, endTime, InfluxCache.ThroughputType.WRITER);

        LocalDateTime startTime2 = LocalDateTime.now();
        LocalDateTime endTime2 = LocalDateTime.now();
        long totalBytes2 = 1000;
        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes2, startTime2, endTime2, InfluxCache.ThroughputType.WRITER);
        Assertions.assertEquals(1, testObj.threadCache.size()); //there is only 1 thread per entry

        //now test the quality of the one read object
        JobMetric metric = testObj.threadCache.get(testThreadId);
        Assertions.assertEquals(metric.getWriteStartTime(), startTime);
        Assertions.assertEquals(metric.getWriteEndTime(), endTime2);
        Assertions.assertEquals(metric.getWrittenBytes(), totalBytes+totalBytes2);
        Assertions.assertEquals(metric.getWriteThroughput(), ODSConstants.computeThroughput(totalBytes+totalBytes2, Duration.between(startTime, endTime2)));
        Assertions.assertEquals(metric.getConcurrency(), 1);
        Assertions.assertEquals(metric.getParallelism(), 1);
        Assertions.assertEquals(metric.getPipelining(), 1);
        Assertions.assertEquals(metric.getStepExecution(), mockedStepExecution);
    }

    @Test
    public void testAddOneReadAndOneWriteMetricOneThread(){
        Mockito.when(mockedThreadPoolManager.concurrencyCount()).thenReturn(1);
        Mockito.when(mockedThreadPoolManager.parallelismCount()).thenReturn(1);
        Mockito.when(mockedStepExecution.getJobParameters().getLong(ODSConstants.PIPELINING)).thenReturn(1L);

        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        long testThreadId = Thread.currentThread().getId(), totalBytes = 100;
        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes, startTime, endTime, InfluxCache.ThroughputType.READER);

        LocalDateTime startTime2 = LocalDateTime.now();
        LocalDateTime endTime2 = LocalDateTime.now();
        long totalBytes2 = 1000;
        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes2, startTime2, endTime2, InfluxCache.ThroughputType.WRITER);
        Assertions.assertEquals(1, testObj.threadCache.size()); //there is only 1 thread per entry

        //now test the quality of the one read object
        JobMetric metric = testObj.threadCache.get(testThreadId);
        Assertions.assertEquals(metric.getReadStartTime(), startTime);
        Assertions.assertEquals(metric.getReadEndTime(), endTime);
        Assertions.assertEquals(metric.getReadBytes(), totalBytes);
        Assertions.assertEquals(metric.getReadThroughput(), ODSConstants.computeThroughput(totalBytes, Duration.between(startTime, endTime)));

        Assertions.assertEquals(metric.getWriteStartTime(), startTime2);
        Assertions.assertEquals(metric.getWriteEndTime(), endTime2);
        Assertions.assertEquals(metric.getWrittenBytes(), totalBytes2);
        Assertions.assertEquals(metric.getWriteThroughput(), ODSConstants.computeThroughput(totalBytes2, Duration.between(startTime2, endTime2)));

        Assertions.assertEquals(metric.getConcurrency(), 1);
        Assertions.assertEquals(metric.getParallelism(), 1);
        Assertions.assertEquals(metric.getPipelining(), 1);
        Assertions.assertEquals(metric.getStepExecution(), mockedStepExecution);
    }

    @Test
    public void testAddOneReadAndOneWriteMetricsTwoDifferentThreads(){
        Mockito.when(mockedThreadPoolManager.concurrencyCount()).thenReturn(1);
        Mockito.when(mockedThreadPoolManager.parallelismCount()).thenReturn(1);
        Mockito.when(mockedStepExecution.getJobParameters().getLong(ODSConstants.PIPELINING)).thenReturn(1L);

        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        long testThreadId = Thread.currentThread().getId(), totalBytes = 100;
        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes, startTime, endTime, InfluxCache.ThroughputType.READER);
        Assertions.assertEquals(1, testObj.threadCache.size());

        LocalDateTime startTime2 = LocalDateTime.now();
        LocalDateTime endTime2 = LocalDateTime.now();
        long totalBytes2 = 1000;
        long testThreadId2 = testThreadId +1;
        testObj.addMetric(testThreadId2, mockedStepExecution, totalBytes2, startTime2, endTime2, InfluxCache.ThroughputType.WRITER);
        Assertions.assertEquals(2, testObj.threadCache.size()); //there is only 1 thread per entry

        JobMetric thread1Metric = this.testObj.threadCache.get(testThreadId);
        Assertions.assertEquals(thread1Metric.getThreadId(), testThreadId);
        Assertions.assertEquals(thread1Metric.getReadThroughput(), ODSConstants.computeThroughput(totalBytes, Duration.between(startTime, endTime)));
        Assertions.assertEquals(thread1Metric.getReadStartTime(), startTime);
        Assertions.assertEquals(thread1Metric.getReadEndTime(), endTime);
        Assertions.assertEquals(thread1Metric.getReadBytes(), totalBytes);

        JobMetric thread2Metric = this.testObj.threadCache.get(testThreadId2);
        Assertions.assertEquals(thread2Metric.getThreadId(), testThreadId2);
        Assertions.assertEquals(thread2Metric.getWriteThroughput(), ODSConstants.computeThroughput(totalBytes, Duration.between(startTime2, endTime2)));
        Assertions.assertEquals(thread2Metric.getWriteStartTime(), startTime2);
        Assertions.assertEquals(thread2Metric.getWriteEndTime(), endTime2);
        Assertions.assertEquals(thread2Metric.getWrittenBytes(), totalBytes2);
    }

    @Test
    public void testAddTwoReadTwoThreadTwoWriteTwoThreads(){
        Mockito.when(mockedThreadPoolManager.concurrencyCount()).thenReturn(1);
        Mockito.when(mockedThreadPoolManager.parallelismCount()).thenReturn(1);
        Mockito.when(mockedStepExecution.getJobParameters().getLong(ODSConstants.PIPELINING)).thenReturn(1L);

        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        long testThreadId = Thread.currentThread().getId(), totalBytes = 100;
        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes, startTime, endTime, InfluxCache.ThroughputType.READER);
        Assertions.assertEquals(1, testObj.threadCache.size());

        LocalDateTime startTime2 = LocalDateTime.now();
        LocalDateTime endTime2 = LocalDateTime.now();
        long totalBytes2 = 1000;
        long testThreadId2 = testThreadId +1;
        testObj.addMetric(testThreadId2, mockedStepExecution, totalBytes2, startTime2, endTime2, InfluxCache.ThroughputType.WRITER);
        Assertions.assertEquals(2, testObj.threadCache.size()); //there is only 1 thread per entry

        JobMetric thread1Metric = this.testObj.threadCache.get(testThreadId);
        Assertions.assertEquals(thread1Metric.getThreadId(), testThreadId);
        Assertions.assertEquals(thread1Metric.getReadThroughput(), ODSConstants.computeThroughput(totalBytes, Duration.between(startTime, endTime)));
        Assertions.assertEquals(thread1Metric.getReadStartTime(), startTime);
        Assertions.assertEquals(thread1Metric.getReadEndTime(), endTime);
        Assertions.assertEquals(thread1Metric.getReadBytes(), totalBytes);

        JobMetric thread2Metric = this.testObj.threadCache.get(testThreadId2);
        Assertions.assertEquals(thread2Metric.getThreadId(), testThreadId2);
        Assertions.assertEquals(thread2Metric.getWriteThroughput(), ODSConstants.computeThroughput(totalBytes, Duration.between(startTime2, endTime2)));
        Assertions.assertEquals(thread2Metric.getWriteStartTime(), startTime2);
        Assertions.assertEquals(thread2Metric.getWriteEndTime(), endTime2);
        Assertions.assertEquals(thread2Metric.getWrittenBytes(), totalBytes2);

    }

    @Test
    public void testRunAggWithEmptyThreadCache(){
        testObj = new InfluxCache(mockedThreadPoolManager);
        JobMetric jobMetric = testObj.aggregateMetric();
        Assertions.assertNull(jobMetric);
    }

    @Test
    public void testRunAggWithOneObject(){
        Mockito.when(mockedThreadPoolManager.concurrencyCount()).thenReturn(1);
        Mockito.when(mockedThreadPoolManager.parallelismCount()).thenReturn(1);
        Mockito.when(mockedStepExecution.getJobParameters().getLong(ODSConstants.PIPELINING)).thenReturn(1L);
        testObj = new InfluxCache(mockedThreadPoolManager);
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        long testThreadId = Thread.currentThread().getId(), totalBytes = 100;
        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes, startTime, endTime, InfluxCache.ThroughputType.READER);

        JobMetric jobMetric = testObj.aggregateMetric();
        JobMetric expectedMetric = this.buildReadExpectedMetric(startTime, endTime, testThreadId, totalBytes, 1, 1, 1);

        Assertions.assertEquals(expectedMetric, jobMetric);
    }

    @Test
    public void testRunAggWithTwoThreadsTwoMetricEach(){
        Mockito.when(mockedThreadPoolManager.concurrencyCount()).thenReturn(1);
        Mockito.when(mockedThreadPoolManager.parallelismCount()).thenReturn(1);
        Mockito.when(mockedStepExecution.getJobParameters().getLong(ODSConstants.PIPELINING)).thenReturn(1L);

        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        long testThreadId = Thread.currentThread().getId(), totalBytes = 100;
        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes, startTime, endTime, InfluxCache.ThroughputType.READER);
        testObj.addMetric(testThreadId, mockedStepExecution, totalBytes, startTime, endTime, InfluxCache.ThroughputType.WRITER);
        Assertions.assertEquals(1, testObj.threadCache.size());

        LocalDateTime startTime2 = LocalDateTime.now();
        LocalDateTime endTime2 = LocalDateTime.now();
        long totalBytes2 = 1000;
        testObj.addMetric(testThreadId+1, mockedStepExecution, totalBytes2, startTime2, endTime2, InfluxCache.ThroughputType.READER);
        testObj.addMetric(testThreadId+1, mockedStepExecution, totalBytes2, startTime2, endTime2, InfluxCache.ThroughputType.WRITER);
        Assertions.assertEquals(2, testObj.threadCache.size()); //there is only 1 thread per entry

        JobMetric realAgg = testObj.aggregateMetric();

        Assertions.assertEquals(mockedStepExecution,realAgg.getStepExecution());
        Assertions.assertEquals(totalBytes+totalBytes2, realAgg.getReadBytes());
        Assertions.assertEquals(ODSConstants.computeThroughput(totalBytes+totalBytes2, Duration.between(startTime, endTime2)), realAgg.getReadThroughput());
        Assertions.assertEquals(ODSConstants.computeThroughput(totalBytes+totalBytes2, Duration.between(startTime, endTime2)), realAgg.getWriteThroughput());
        Assertions.assertEquals(totalBytes+totalBytes2, realAgg.getWrittenBytes());
    }
}
