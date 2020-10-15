package org.onedatashare.transferservice.odstransferservice.service.listner;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;


public class JobCompletionListener extends JobExecutionListenerSupport {
    Logger logger = LoggerFactory.getLogger(JobCompletionListener.class);

    @SneakyThrows
    @Override
    public void beforeJob(JobExecution jobExecution) {
        logger.info("BEFOR JOB---------------------" + System.currentTimeMillis());
    }

    @SneakyThrows
    @Override
    public void afterJob(JobExecution jobExecution) {
        logger.info("After JOB--------------------" + System.currentTimeMillis());
    }
}