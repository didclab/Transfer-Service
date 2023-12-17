package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.onedatashare.transferservice.odstransferservice.service.VfsExpander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * Transfer controller with to initiate transfer request
 */
@RestController
@RequestMapping("/api/v1/transfer")
public class TransferController {

    Logger logger = LoggerFactory.getLogger(TransferController.class);

    @Autowired
    JobControl jc;

    @Autowired
    JobLauncher asyncJobLauncher;

    @Autowired
    JobParamService jobParamService;

    @Autowired
    VfsExpander vfsExpander;

    @Autowired
    JobExplorer jobExplorer;

    Set<Long> jobIds;
    public TransferController(Set<Long> jobIds){
        this.jobIds = jobIds;
    }


    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @Async
    public ResponseEntity<String> start(@RequestBody TransferJobRequest request) throws Exception {
        logger.info("Controller Entry point");
        if (request.getSource().getType().equals(EndpointType.vfs)) {
            List<EntityInfo> fileExpandedList = vfsExpander.expandDirectory(request.getSource().getInfoList(), request.getSource().getFileSourcePath());
            request.getSource().setInfoList(new ArrayList<>(fileExpandedList));
        }
        JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
        jc.setRequest(request);
        Job job = jc.concurrentJobDefinition();
        JobExecution jobExecution = asyncJobLauncher.run(job, parameters);
        return ResponseEntity.status(HttpStatus.OK).body("Your batch job has been submitted with \n ID: " + jobExecution.getJobId());
    }

    @RequestMapping(value = "/pause", method = RequestMethod.POST)
    @Async
    public ResponseEntity<String> pause() throws Exception{
        logger.info("Pause Controller Entry point");
        Long runningJobId = 0L;
        for(Long jobId : jobIds){
            JobExecution jobExecution = jobExplorer.getJobExecution(jobId);
            if(jobExecution != null && jobExecution.isRunning()){
                runningJobId = jobId;
                break;
            }
        }

        if(runningJobId == 0L){
            return ResponseEntity.status(HttpStatus.OK).body("No running job found");
        }


        return ResponseEntity.status(HttpStatus.OK).body("Your batch job with id "+runningJobId+"has been paused");
    }

}

