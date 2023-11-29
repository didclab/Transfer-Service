package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.BatchJobData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;


@RequestMapping("/api/v1/job")
@RestController
public class JobMonitor {

    private final JobExplorer jobExplorer;
    private Set<Long> jobIds;

    Logger logger = LoggerFactory.getLogger(JobMonitor.class);

    public JobMonitor(JobExplorer jobExplorer, Set<Long> jobIds) {
        this.jobExplorer = jobExplorer;
        this.jobIds = jobIds;
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

    @GetMapping("/ids")
    public ResponseEntity<List<Long>> getJobIdsRun() {
        logger.info("Listing Job Ids");
        return ResponseEntity.ok(new ArrayList<>(this.jobIds));
    }

//    @GetMapping("/uuid")
//    public ResponseEntity<List<UUID>> getJobExec(@RequestParam Optional<List<Long>> jobIds){
//        List<UUID> jobUuids = new ArrayList<>();
//        if(jobIds.isPresent()){
//            for(Long jobId: jobIds.get()){
//                JobExecution jobExecution = this.jobExplorer.getJobExecution(jobId);
//                JobParameters jobParameters = jobExecution.getJobParameters();
//                String jobUuid = jobParameters.getString(ODSConstants.JOB_UUID);
//                jobUuids.add(UUID.fromString(jobUuid));
//            }
//        }else{
//            for(Long jobId : this.jobIds){
//                JobExecution jobExecution = this.jobExplorer.getJobExecution(jobId);
//                JobParameters jobParameters = jobExecution.getJobParameters();
//                String jobUuid = jobParameters.getString(ODSConstants.JOB_UUID);
//                jobUuids.add(UUID.fromString(jobUuid));
//            }
//        }
//        return ResponseEntity.ok(jobUuids);
//    }
}
