package org.onedatashare.transferservice.odstransferservice.service.listner;

import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.OptimizerCreateRequest;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.OptimizerDeleteRequest;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.onedatashare.transferservice.odstransferservice.service.ConnectionBag;
import org.onedatashare.transferservice.odstransferservice.service.OptimizerService;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;


@Service
public class JobCompletionListener extends JobExecutionListenerSupport {
    Logger logger = LoggerFactory.getLogger(JobCompletionListener.class);

    ConnectionBag connectionBag;

    MetricsCollector metricsCollector;

    OptimizerService optimizerService;

    ThreadPoolManager threadPoolManager;

    @Value("${spring.application.name}")
    private String appName;

    @Value("${transfer.service.parallelism}")
    int maxParallel;

    @Value("${transfer.service.concurrency}")
    int maxConc;

    @Value("${transfer.service.pipelining}")
    int maxPipe;
    boolean optimizerEnable;

    public JobCompletionListener(ThreadPoolManager threadPoolManager, OptimizerService optimizerService, MetricsCollector metricsCollector, ConnectionBag connectionBag) {
        this.threadPoolManager = threadPoolManager;
        this.optimizerService = optimizerService;
        this.metricsCollector = metricsCollector;
        this.connectionBag = connectionBag;
        this.optimizerEnable = false;
    }


    @Override
    public void beforeJob(JobExecution jobExecution) {
        logger.info("*****Job Execution start Time***** : {}", jobExecution.getStartTime());
        long fileCount = jobExecution.getJobParameters().getLong(ODSConstants.FILE_COUNT);
        String optimizerType = jobExecution.getJobParameters().getString(ODSConstants.OPTIMIZER);
        if(optimizerType != null){
            if(!optimizerType.equals("None")) {
                OptimizerCreateRequest createRequest = new OptimizerCreateRequest(appName, maxConc, maxParallel, maxPipe, optimizerType, fileCount);
                optimizerService.createOptimizerBlocking(createRequest);
                this.optimizerEnable = true;
            }
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        logger.info("*****Job Execution End Time**** : {}", jobExecution.getEndTime());
        LocalDateTime startTime = Instant.ofEpochMilli(jobExecution.getStartTime().getTime())
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        LocalDateTime endTime = Instant.ofEpochMilli(jobExecution.getEndTime().getTime())
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        logger.info("Total Job Time in seconds: {}", Duration.between(startTime, endTime).toSeconds());
        connectionBag.closePools();
        threadPoolManager.clearJobPool();
        if(this.optimizerEnable){
            this.optimizerService.deleteOptimizerBlocking(new OptimizerDeleteRequest(appName));
            this.optimizerEnable = false;
        }
    }
}

