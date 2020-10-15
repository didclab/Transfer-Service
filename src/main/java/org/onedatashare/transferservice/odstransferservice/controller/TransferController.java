package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfoMap;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;

import java.util.*;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

/**
 * Transfer controller with to initiate transfer request
 */
@RestController
@RequestMapping("/api/v1/transfer")
public class TransferController {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    JobControl jc;

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    JobLauncher asyncJobLauncher;

    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @Async
    public ResponseEntity<String> start(@RequestBody TransferJobRequest request) throws Exception {
        JobParameters parameters = translate(new JobParametersBuilder(), request);
        Map<String,Long> hm = new HashMap<>();
        for(EntityInfo ei:request.getSource().getInfoList()){
            hm.put(ei.getPath(),ei.getSize());
        }
        EntityInfoMap.setHm(hm);
        //System.out.println(job+"---->>>"+parameters);
        jc.setRequest(request);
        jc.setChunckSize(SIXTYFOUR_KB); //64kb.
        asyncJobLauncher.run(jc.createJobDefinition(), parameters);
        return ResponseEntity.status(HttpStatus.OK).body("Your batch job has been submitted with \n ID: " + request.getId());
    }

    public JobParameters translate(JobParametersBuilder builder, TransferJobRequest request) {
        System.out.println(request.toString());
        builder.addLong(TIME,System.currentTimeMillis());
        builder.addString(SOURCE_CREDENTIAL_ID, request.getSource().getCredId());
        builder.addString(DEST_CREDENTIAL_ID, request.getDestination().getCredId());
        builder.addString(SOURCE_BASE_PATH, request.getSource().getInfo().getPath());
        builder.addString(DEST_BASE_PATH, request.getDestination().getInfo().getPath());
        builder.addString(INFO_LIST, request.getSource().getInfoList().toString());
        builder.addString(SOURCE_ACCOUNT_ID_PASS, request.getSource().getCredential().getAccountId() + ":" + request.getSource().getCredential().getPassword());
        builder.addString(DESTINATION_ACCOUNT_ID_PASS, request.getDestination().getCredential().getAccountId() + ":" + request.getDestination().getCredential().getPassword());
        builder.addString(PRIORITY,String.valueOf(request.getPriority()));
        builder.addString(OWNER_ID,request.getOwnerId());
        return builder.toJobParameters();
    }
}

