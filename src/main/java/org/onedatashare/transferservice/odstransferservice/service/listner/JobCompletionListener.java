package org.onedatashare.transferservice.odstransferservice.service.listner;

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
            optimizerService.createOptimizerBlocking(new OptimizerCreateRequest(appName, maxConc, maxParallel, maxPipe));
            this.future = optimizerTaskScheduler.scheduleWithFixedDelay(optimizerCron, interval);
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        logger.info("After JOB------------------present time--" + jobExecution.getEndTime());
        connectionBag.closePools();
        threadPoolManager.clearJobPool();
        if(this.optimizerEnable){
            this.future.cancel(true);
            metricCache.clearCache();
            optimizerService.deleteOptimizerBlocking(new OptimizerDeleteRequest(appName));
        }
    }
}

