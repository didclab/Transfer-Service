package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.JobRequestDTO;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.JobQueryService;
import org.springframework.batch.core.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
    public ResponseEntity<String> getStatus(@RequestBody JobRequestDTO jobRequestDTO) {
        String jobName = jobRequestDTO.getJobName();
        String isDirectory = jobRequestDTO.getIsDirectory();
        boolean directory = Boolean.parseBoolean(isDirectory);
        BatchStatus status;
        JobInstance jobInstance = jobQueryImplementation.getLastJobInstance(jobName);
        if(directory) {
            JobExecution jobExecution = jobQueryImplementation.getJobExecution(jobInstance);
            status = jobExecution.getStatus();
        }
        else {
            String stepName = jobRequestDTO.getStepName();
            StepExecution stepExecution = jobQueryImplementation.getLastStepExecution(jobInstance,stepName);
            status = stepExecution.getStatus();
        }
        return ResponseEntity.status(HttpStatus.OK).body(status.toString());
    }

    @RequestMapping(value = "/job_start_time", method = RequestMethod.POST)
    public ResponseEntity<String> getStartTime(@RequestBody JobRequestDTO jobRequestDTO) {
        String jobName = jobRequestDTO.getJobName();
        String isDirectory = jobRequestDTO.getIsDirectory();
        boolean directory = Boolean.parseBoolean(isDirectory);
        Date startTime;
        JobInstance jobInstance = jobQueryImplementation.getLastJobInstance(jobName);
        if(directory){
            JobExecution jobExecution = jobQueryImplementation.getJobExecution(jobInstance);
            startTime = jobExecution.getStartTime();
        }
        else {
            String stepName = jobRequestDTO.getStepName();
            StepExecution stepExecution = jobQueryImplementation.getLastStepExecution(jobInstance,stepName);
            startTime = stepExecution.getStartTime();
        }
        return ResponseEntity.status(HttpStatus.OK).body(startTime.toString());
    }

    @RequestMapping(value = "/job_finish_time", method = RequestMethod.POST)
    public ResponseEntity<String> getFinishTime(@RequestBody JobRequestDTO jobRequestDTO) {
        String jobName = jobRequestDTO.getJobName();
        String isDirectory = jobRequestDTO.getIsDirectory();
        boolean directory = Boolean.parseBoolean(isDirectory);
        Date finishTime;
        JobInstance jobInstance = jobQueryImplementation.getLastJobInstance(jobName);
        if(directory){
            JobExecution jobExecution = jobQueryImplementation.getJobExecution(jobInstance);
            finishTime = jobExecution.getEndTime();
        }
        else {
            String stepName = jobRequestDTO.getStepName();
            StepExecution stepExecution = jobQueryImplementation.getLastStepExecution(jobInstance,stepName);
            finishTime = stepExecution.getEndTime();
        }
        return ResponseEntity.status(HttpStatus.OK).body(finishTime.toString());
    }

    @RequestMapping(value = "/job_exit_message", method = RequestMethod.POST)
    public ResponseEntity<String> getExitMessage(@RequestBody JobRequestDTO jobRequestDTO) {
        String jobName = jobRequestDTO.getJobName();
        String isDirectory = jobRequestDTO.getIsDirectory();
        boolean directory = Boolean.parseBoolean(isDirectory);
        ExitStatus executionExitStatus;
        JobInstance jobInstance = jobQueryImplementation.getLastJobInstance(jobName);
        if(directory){
            JobExecution jobExecution = jobQueryImplementation.getJobExecution(jobInstance);
            executionExitStatus = jobExecution.getExitStatus();
        }
        else {
            String stepName = jobRequestDTO.getStepName();
            StepExecution stepExecution = jobQueryImplementation.getLastStepExecution(jobInstance,stepName);
            executionExitStatus = stepExecution.getExitStatus();
        }
        return ResponseEntity.status(HttpStatus.OK).body(executionExitStatus.toString());
    }
}

