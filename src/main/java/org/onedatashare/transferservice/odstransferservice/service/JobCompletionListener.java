package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolContract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Set;


@Service
public class JobCompletionListener implements JobExecutionListener {
    private final ThreadPoolContract threadPool;
    private Set<Long> jobIds;
    Logger logger = LoggerFactory.getLogger(JobCompletionListener.class);

    ConnectionBag connectionBag;

    MetricsCollector metricsCollector;

    boolean optimizerEnable;

    @Autowired
    FileTransferNodeRegistrationService fileTransferNodeRegistrationService;

    public JobCompletionListener(MetricsCollector metricsCollector, ConnectionBag connectionBag, ThreadPoolContract threadPool, Set<Long> jobIds) {
        this.metricsCollector = metricsCollector;
        this.connectionBag = connectionBag;
        this.optimizerEnable = false;
        this.threadPool = threadPool;
        this.jobIds = jobIds;
    }


    @Override
    @Async
    public void beforeJob(JobExecution jobExecution) {
        logger.info("*****Job Execution start Time***** : {} with jobId={}", jobExecution.getStartTime(), jobExecution.getJobId());
        this.jobIds.add(jobExecution.getJobId());
        try {
            this.fileTransferNodeRegistrationService.updateRegistrationInHazelcast(jobExecution);
        } catch (JsonProcessingException e) {
            logger.error("Failed to update status of FTN inside of Hazelcast for job start. Exception \n {}", e.getMessage());
        }
    }

    @Override
    @Async
    public void afterJob(JobExecution jobExecution) {
        logger.info("*****Job Execution End Time**** : {}", jobExecution.getEndTime());
        logger.info("Total Job Time in seconds: {}", Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toSeconds());
        connectionBag.closePools();
        this.threadPool.clearPools();
        System.gc();
        try {
            this.fileTransferNodeRegistrationService.updateRegistrationInHazelcast(null);
        } catch (JsonProcessingException e) {
            logger.error("Failed to update status of FTN inside of Hazelcast for job end. Exception \n {}", e.getMessage());
        }
    }
}

