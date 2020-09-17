package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

/**
 * Transfer controller with to initiate transfer request
 */
@RestController
@RequestMapping("/api/v1/transfer")
public class TransferController {

    Logger logger = LoggerFactory.getLogger(TransferController.class);

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    Job job;

    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @Async
    public ResponseEntity<String> start(@RequestBody TransferJobRequest request) throws Exception {
        JobParameters parameters = translate(new JobParametersBuilder(), request);
        //System.out.println(job+"---->>>"+parameters);
        List<EntityInfo> transferFiles = request.getSource().getInfoList();
        List<JobParameters> params = fileToJob(request);
        for(JobParameters par: params){
            jobLauncher.run(job, par);
        }
        return ResponseEntity.status(HttpStatus.OK).body("Your batch job has been submitted with \n ID: " + request.getId());
    }

    public List<JobParameters> fileToJob(TransferJobRequest request){
        List<JobParameters> ret = new ArrayList<>();
        for(EntityInfo info: request.getSource().getInfoList()){
            JobParametersBuilder builder = new JobParametersBuilder();
            builder.addLong(TIME, System.currentTimeMillis());
            builder.addString(FILE_SIZE, Long.toString(info.getSize()));
            builder.addString(FILE_PATH, info.getPath());
            builder.addString(FILE_ID, info.getId());
            ret.add(builder.toJobParameters());
        }
        return ret;
    }

    public JobParameters translate(JobParametersBuilder builder, TransferJobRequest request) {
        System.out.println(request.toString());
        builder.addLong("time",System.currentTimeMillis());
        builder.addString("sourceAccountIdPass", request.getSource().getCredential().getAccountId()
                + ":" + request.getSource().getCredential().getPassword());
        builder.addString("destinationAccountIdPass", request.getDestination().getCredential().getAccountId()
                + ":" + request.getDestination().getCredential().getPassword());
        builder.addString("sourceBasePath", request.getSource().getInfo().getPath());
        builder.addString("destBasePath", request.getDestination().getInfo().getPath());
        builder.addString("fileName", request.getSource().getInfoList().get(0).getPath());
        return builder.toJobParameters();
    }
}

