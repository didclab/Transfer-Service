package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.TransferService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Transfer controller with to initiate transfer request
 */
@RestController
@RequestMapping("/api/v1/transfer")
public class TransferController {

    Logger logger = LoggerFactory.getLogger(TransferController.class);

    @Qualifier("asyncJobLauncher")
    JobLauncher asyncJobLauncher;

    @Autowired
    Job job;

    @RequestMapping(value = "/start", method = RequestMethod.POST)
    public ResponseEntity<String> start(@RequestBody TransferJobRequest request) throws Exception {
        logger.info("Inside TransferController");
        asyncJobLauncher.run(job, translate(new JobParametersBuilder(), request));
        return ResponseEntity.status(HttpStatus.OK).body("Your batch job has been submitted with \n ID: " + request.getId());
    }

    public JobParameters translate(JobParametersBuilder builder, TransferJobRequest request) {
        StringBuilder sb = new StringBuilder("");
        String basePath = request.getSource().getInfo().getPath();
        for (EntityInfo entityInfo : request.getSource().getInfoList()) {
            sb.append(basePath).append(entityInfo.getPath()).append("<::>");
        }
        builder.addString("listToTransfer", sb.toString());
        builder.addString("source", request.getSource().toString());
        builder.addString("dest", request.getDestination().toString());
        builder.addString("priority", Integer.toString(request.getPriority()));
        builder.addString("transfer-options", request.getOptions().toString());
        builder.addString("id", request.getId());
        builder.addString("ownerId", request.getOwnerId());
        return builder.toJobParameters();
    }
}

