package org.onedatashare.transferservice.odstransferservice.controller;

import com.netflix.discovery.converters.Auto;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobFactory;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.jsr.configuration.xml.JobFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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
        logger.info("Inside TransferController");
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
            builder.addString("fileSize", Long.toString(info.getSize()));
            builder.addString("filePath", info.getPath());
            builder.addString("id",info.getId());
            ret.add(builder.toJobParameters());
        }
        return ret;
    }

    public JobParameters translate(JobParametersBuilder builder, TransferJobRequest request) {
        System.out.println(request.toString());
        StringBuilder sb = new StringBuilder("");
        String basePath = request.getSource().getInfo().getPath();
        for (EntityInfo entityInfo : request.getSource().getInfoList()) {
            sb.append(basePath).append(entityInfo.getPath()).append("<::>");
        }
        builder.addLong("time",System.currentTimeMillis());
        builder.addString("listToTransfer", sb.toString());
//        builder.addString("source", request.getSource().toString());
//        builder.addString("dest", request.getDestination().toString());
        builder.addString("destPath", request.getDestination().getInfo().getPath());
//        builder.addString("priority", Integer.toString(request.getPriority()));
//        builder.addString("transfer-options", request.getOptions().toString());
//        builder.addString("id", request.getId());
//        builder.addString("ownerId", request.getOwnerId());

        return builder.toJobParameters();
    }
}

