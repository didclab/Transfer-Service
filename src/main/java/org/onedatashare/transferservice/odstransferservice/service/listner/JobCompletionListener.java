package org.onedatashare.transferservice.odstransferservice.service.listner;

import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.service.ConnectionBag;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;


@Component
public class JobCompletionListener extends JobExecutionListenerSupport {
    Logger logger = LoggerFactory.getLogger(JobCompletionListener.class);

    @Autowired
    ConnectionBag connectionBag;

    @Autowired
    MetricsCollector metricsCollector;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        logger.info("BEFOR JOB-------------------present time--" + System.currentTimeMillis());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        logger.info("After JOB------------------present time--" + System.currentTimeMillis());
        JobParameters jobParameters = jobExecution.getJobParameters();
        long jobCompletionTime = Duration.between(jobExecution.getStartTime().toInstant(), jobExecution.getEndTime().toInstant()).toMillis();
        double throughput = jobParameters.getLong(JOB_SIZE)/jobCompletionTime; //todo - null check
        logger.info("Job throughput (bytes/ms): " + throughput);
        JobMetric jobMetric = new JobMetric();
        jobMetric.setJobId(jobExecution.getJobId().toString());
        jobMetric.setConcurrency(jobParameters.getLong(CONCURRENCY));
        jobMetric.setParallelism(jobParameters.getLong(PARALLELISM));
        jobMetric.setPipelining(jobParameters.getLong(PIPELINING));
        jobMetric.setThroughput(throughput);
        metricsCollector.collectJobMetrics(jobMetric);
        connectionBag.closePools();
    }
}

