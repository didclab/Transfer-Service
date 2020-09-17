package org.onedatashare.transferservice.odstransferservice.service.listner;

import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.StreamOutput;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;

import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

public class JobCompletionListener extends JobExecutionListenerSupport {

    String dBasePath;
    String fName;
    String dAccountIdPass;
    String url;

    @SneakyThrows
    @Override
    public void beforeJob(JobExecution jobExecution){
        System.out.println("BEFOR JOB-----------"+jobExecution.getJobParameters());
        dBasePath = jobExecution.getJobParameters().getString("destBasePath");
        fName = jobExecution.getJobParameters().getString("fileName");
        dAccountIdPass = jobExecution.getJobParameters().getString("destinationAccountIdPass");
        url = dBasePath.substring(0, 6) + dAccountIdPass + "@" + dBasePath.substring(6) + fName;
        URL u = new URL(url);
        URLConnection conn = u.openConnection();
        StreamOutput.setOutputStream(conn.getOutputStream());


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