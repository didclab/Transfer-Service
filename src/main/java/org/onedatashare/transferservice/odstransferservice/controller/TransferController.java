package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.springframework.batch.core.JobExecution;
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

    JobControl jobControl;

    public TransferController(JobControl jobControl) {
        this.jobControl = jobControl;
    }

    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @Async
    public ResponseEntity<Long> start(@RequestBody TransferJobRequest request) throws Exception {
        JobExecution jobExecution = this.jobControl.runJob(request);
        return ResponseEntity.ok(jobExecution.getJobId());
    }
}

