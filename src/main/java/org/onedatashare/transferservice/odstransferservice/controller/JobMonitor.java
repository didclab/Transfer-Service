package org.onedatashare.transferservice.odstransferservice.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.onedatashare.transferservice.odstransferservice.model.JobDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.Optional;

@RequestMapping("/api/v1/job")
@RestController
public class JobMonitor {

    private final JobExplorer jobExplorer;

    Logger logger = LoggerFactory.getLogger(JobMonitor.class);

    public JobMonitor(JobExplorer jobExplorer) {
        this.jobExplorer = jobExplorer;
    }

    @GetMapping("/status")
    public ResponseEntity<BatchStatus> getJobStatus(@RequestParam("jobId") Optional<Long> jobId) {
        if (jobId.isPresent()) {
            logger.info(String.valueOf(jobId.get()));
            JobExecution jobExecution = this.jobExplorer.getJobExecution(jobId.get());
            if (jobExecution == null) {
                return ResponseEntity.notFound().build();
            }
            logger.info(jobExecution.toString());
            return ResponseEntity.ok(jobExecution.getStatus());
        } else {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/execution")
    public ResponseEntity<JobDescription> getJobExecution(@RequestParam("jobId") Optional<Long> jobId) throws JsonProcessingException {
        if (jobId.isPresent()) {
            logger.info(jobId.get().toString());
            JobExecution jobExecution = this.jobExplorer.getJobExecution(jobId.get());
            if(jobExecution == null) return null;
            JobDescription jobDescription = JobDescription.convertFromJobExecution(jobExecution);
            return ResponseEntity.ok(jobDescription);
        } else {
            return null;
        }
    }
}
