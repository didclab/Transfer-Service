package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TransferService {

    static TransferJobRequest transferJobRequest = null;

    Logger logger = LoggerFactory.getLogger(TransferService.class);

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    Job processJob;

    public static TransferJobRequest getTransferJobRequest(){
        return transferJobRequest;
    }
    public String submit(TransferJobRequest request) throws Exception {
        logger.info("Inside submit function");
        transferJobRequest = request;

        JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
                .toJobParameters();
        jobLauncher.run(processJob, jobParameters);

        return "Batch job has been invoked";
    }
}
