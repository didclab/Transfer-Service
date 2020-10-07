package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.JobRequestDTO;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.JobQueryService;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
@RequestMapping("/api/v1/query")
public class DatabaseController {
    @Autowired
    JobQueryService jobQueryImplementation;
    @RequestMapping(value = "/job_status", method = RequestMethod.POST)
    @Async
    public ResponseEntity<String> getStatus(@RequestBody JobRequestDTO jobRequestDTO) throws Exception {
        String jobName = jobRequestDTO.getJobName();
        JobInstance jobInstance = jobQueryImplementation.getLastJobInstance(jobName);
        JobExecution jobExecution = jobQueryImplementation.getJobExecution(jobInstance);
        BatchStatus jobStatus = jobExecution.getStatus();
        return ResponseEntity.status(HttpStatus.OK).body(jobStatus.toString());
    }

    @RequestMapping(value = "/job_start_time", method = RequestMethod.POST)
    @Async
    public ResponseEntity<String> getStartTime(@RequestBody JobRequestDTO jobRequestDTO) throws Exception {
        String jobName = jobRequestDTO.getJobName();
        JobInstance jobInstance = jobQueryImplementation.getLastJobInstance(jobName);
        JobExecution jobExecution = jobQueryImplementation.getJobExecution(jobInstance);
        Date jobStartTime = jobExecution.getStartTime();
        return ResponseEntity.status(HttpStatus.OK).body(jobStartTime.toString());
    }

    @RequestMapping(value = "/job_finish_time", method = RequestMethod.POST)
    @Async
    public ResponseEntity<String> getFinishTime(@RequestBody JobRequestDTO jobRequestDTO) throws Exception {
        String jobName = jobRequestDTO.getJobName();
        JobInstance jobInstance = jobQueryImplementation.getLastJobInstance(jobName);
        JobExecution jobExecution = jobQueryImplementation.getJobExecution(jobInstance);
        Date jobEndTime = jobExecution.getEndTime();
        return ResponseEntity.status(HttpStatus.OK).body(jobEndTime.toString());
    }

    @RequestMapping(value = "/job_exit_message", method = RequestMethod.POST)
    @Async
    public ResponseEntity<String> getExitMessage(@RequestBody JobRequestDTO jobRequestDTO) throws Exception {
        String jobName = jobRequestDTO.getJobName();
        JobInstance jobInstance = jobQueryImplementation.getLastJobInstance(jobName);
        JobExecution jobExecution = jobQueryImplementation.getJobExecution(jobInstance);
        ExitStatus jobExecutionExitStatus = jobExecution.getExitStatus();
        return ResponseEntity.status(HttpStatus.OK).body(jobExecutionExitStatus.toString());
    }
}

