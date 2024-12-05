package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.BatchJobData;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;


@RequestMapping("/api/v1/job")
@RestController
public class JobMonitor {

    private final JobExplorer jobExplorer;
    private final JobControl jobControl;

    Logger logger = LoggerFactory.getLogger(JobMonitor.class);

    public JobMonitor(JobExplorer jobExplorer, JobControl jobControl) {
        this.jobControl = jobControl;
        this.jobExplorer = jobExplorer;
    }

    @GetMapping("/execution")
    public ResponseEntity<BatchJobData> getJobExecution(@RequestParam("jobId") Optional<Long> jobId) {
        if (jobId.isPresent()) {
            logger.info(jobId.get().toString());
            JobExecution jobExecution = this.jobExplorer.getJobExecution(jobId.get());
            if (jobExecution == null) {
                return ResponseEntity.ok(BatchJobData.builder().build());
            }
            return ResponseEntity.ok(BatchJobData.convertFromJobExecution(jobExecution));
        } else {
            return ResponseEntity.ok(BatchJobData.builder().build());
        }
    }

    @GetMapping("/latest")
    public ResponseEntity<BatchJobData> getLatestJobExecution() {
        JobExecution jobExecution = this.jobControl.getLatestJobExecution();
        if(jobExecution == null) {return ResponseEntity.ok(null);}
        return ResponseEntity.ok(BatchJobData.convertFromJobExecution(jobExecution));
    }
}
