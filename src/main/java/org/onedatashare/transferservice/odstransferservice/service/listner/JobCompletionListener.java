package org.onedatashare.transferservice.odstransferservice.service.listner;

import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.StreamInput;
import org.onedatashare.transferservice.odstransferservice.model.StreamOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class JobCompletionListener extends JobExecutionListenerSupport {
    Logger logger = LoggerFactory.getLogger(JobCompletionListener.class);

    StreamOutput streamOutput;
    StreamInput streamInput;

    String sServerName;
    String dServerName;
    String sAccountId;
    String dAccountId;
    String sPass;
    String dPass;
    int sPort;
    int dPort;
    String fName;

    String sBasePath;
    String dBasePath;


    @SneakyThrows
    @Override
    public void beforeJob(JobExecution jobExecution) {
        logger.info("BEFOR JOB-----------" + jobExecution.getJobParameters());
        Thread t = Thread.currentThread();
        String name = t.getName();
        logger.info("Thread name= " + name);
        sBasePath = jobExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        dBasePath = jobExecution.getJobParameters().getString(DEST_BASE_PATH);
        fName = jobExecution.getJobParameters().getString(FILE_NAME);
        String[] sAccountIdPass = jobExecution.getJobParameters().getString(SOURCE_ACCOUNT_ID_PASS).split(":");
        String[] dAccountIdPass = jobExecution.getJobParameters().getString(DESTINATION_ACCOUNT_ID_PASS).split(":");
        String[] sCredential = jobExecution.getJobParameters().getString(SOURCE_CREDENTIAL_ID).split(":");
        String[] dCredential = jobExecution.getJobParameters().getString(DEST_CREDENTIAL_ID).split(":");
        sAccountId = sAccountIdPass[0];
        sPass = sAccountIdPass[1];
        dAccountId = dAccountIdPass[0];
        dPass = dAccountIdPass[1];
        sServerName = sCredential[0];
        sPort = Integer.parseInt(sCredential[1]);
        dServerName = dCredential[0];
        dPort = Integer.parseInt(dCredential[1]);
        streamInput = new StreamInput();
        streamOutput = new StreamOutput();
        streamInput.clientCreateSource(sServerName, sPort, sAccountId, sPass, sBasePath.substring(13 + sAccountId.length() + sPass.length()));
        streamOutput.clientCreateDest(dServerName, dPort, dAccountId, dPass, dBasePath.substring(13 + dAccountId.length() + dPass.length()));
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