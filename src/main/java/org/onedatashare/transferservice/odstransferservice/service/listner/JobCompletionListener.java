package org.onedatashare.transferservice.odstransferservice.service.listner;

import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.StreamInput;
import org.onedatashare.transferservice.odstransferservice.model.StreamOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;

public class JobCompletionListener extends JobExecutionListenerSupport {
    Logger logger = LoggerFactory.getLogger(JobCompletionListener.class);

    String dBasePath;
    String fName;
    String dAccountIdPass;
    String url;
    StreamOutput streamOutput;
    StreamInput streamInput;

    @SneakyThrows
    @Override
    public void beforeJob(JobExecution jobExecution) {
        logger.info("BEFOR JOB-----------" + jobExecution.getJobParameters());
        Thread t = Thread.currentThread();
        String name = t.getName();
        logger.info("Thread name= " + name);
        dBasePath = jobExecution.getJobParameters().getString("destBasePath");
        fName = jobExecution.getJobParameters().getString("fileName");
        dAccountIdPass = jobExecution.getJobParameters().getString("destinationAccountIdPass");
        url = dBasePath.substring(0, 6) + dAccountIdPass + "@" + dBasePath.substring(6) + fName;
        streamInput = new StreamInput();
        streamOutput = new StreamOutput();
        streamInput.clientCreateSource();
        streamOutput.clientCreateDest();
    }

    @SneakyThrows
    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("After JOB-----------");
        StreamOutput.getOutputStream().close();
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            System.out.println("BATCH JOB COMPLETED SUCCESSFULLY");
        }
    }


}