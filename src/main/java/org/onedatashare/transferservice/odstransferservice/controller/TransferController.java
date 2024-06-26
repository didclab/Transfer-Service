package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


/**
 * Transfer controller with to initiate transfer request
 */
@RestController
@RequestMapping("/api/v1/transfer")
public class TransferController {

    Logger logger = LoggerFactory.getLogger(TransferController.class);

    JobControl jc;

    JobLauncher jobLauncher;

    JobParamService jobParamService;

    public TransferController(JobControl jobControl, JobLauncher jobLauncher, JobParamService jobParamService) {
        this.jc = jobControl;
        this.jobLauncher = jobLauncher;
        this.jobParamService = jobParamService;

    }

    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @Async
    public ResponseEntity<String> start(@RequestBody TransferJobRequest request) throws Exception {
        logger.info("Controller Entry point");
        JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
        Job job = jc.concurrentJobDefinition(request);
        JobExecution jobExecution = jobLauncher.run(job, parameters);
        return ResponseEntity.status(HttpStatus.OK).body("Your batch job has been submitted with \n ID: " + jobExecution.getJobId());
    }
}

