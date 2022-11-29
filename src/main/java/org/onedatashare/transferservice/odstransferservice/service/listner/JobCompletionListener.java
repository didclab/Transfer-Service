package org.onedatashare.transferservice.odstransferservice.service.listner;

import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.OptimizerCreateRequest;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.OptimizerDeleteRequest;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.onedatashare.transferservice.odstransferservice.service.ConnectionBag;
import org.onedatashare.transferservice.odstransferservice.service.MetricCache;
import org.onedatashare.transferservice.odstransferservice.service.OptimizerCron;
import org.onedatashare.transferservice.odstransferservice.service.OptimizerService;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

import java.util.concurrent.ScheduledFuture;


@Component
public class JobCompletionListener extends JobExecutionListenerSupport {
    Logger logger = LoggerFactory.getLogger(JobCompletionListener.class);

    @Autowired
    ConnectionBag connectionBag;

    @Autowired
    MetricsCollector metricsCollector;

    @Autowired
    OptimizerService optimizerService;

    @Autowired
    OptimizerCron optimizerCron;

    @Autowired
    ThreadPoolTaskScheduler optimizerTaskScheduler;

    @Autowired
    ThreadPoolManager threadPoolManager;

    @Value("${spring.application.name}")
    private String appName;

    @Value("${optimizer.interval}")
    Integer interval;

    @Value("${optimizer.enable}")
    private boolean optimizerEnable;

    @Value("${transfer.service.parallelism}")
    int maxParallel;

    @Value("${transfer.service.concurrency}")
    int maxConc;

    @Value("${transfer.service.pipelining}")
    int maxPipe;

    @Autowired
    MetricCache metricCache;

    private ScheduledFuture<?> future;


    @Override
    public void beforeJob(JobExecution jobExecution) {
        logger.info("BEFOR JOB-------------------present time--" + jobExecution.getStartTime());
        if(this.optimizerEnable){
            String optimizerType = jobExecution.getJobParameters().getString(ODSConstants.OPTIMIZER);
            long fileCount = jobExecution.getJobParameters().getLong(ODSConstants.FILE_COUNT);
            OptimizerCreateRequest createRequest = new OptimizerCreateRequest(appName, maxConc, maxParallel, maxPipe, optimizerType, fileCount);
            try{
                optimizerService.createOptimizerBlocking(createRequest);
                this.future = optimizerTaskScheduler.scheduleWithFixedDelay(optimizerCron, interval);
            } catch (RestClientException e){
                logger.error("Failed to create the optimizer: {}", createRequest);
            }
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        logger.info("After JOB------------------present time--" + jobExecution.getEndTime());
        connectionBag.closePools();
        threadPoolManager.clearJobPool();
        if(this.optimizerEnable){
            try{
                optimizerService.deleteOptimizerBlocking(new OptimizerDeleteRequest(appName));
                this.future.cancel(true);
                metricCache.clearCache();
            } catch (RestClientException e){
                logger.error("Failed to delete optimizer: {}", appName);
            }
        }
    }
}

