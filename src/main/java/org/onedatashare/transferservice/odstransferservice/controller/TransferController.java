package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.CredentialGroup;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.CrudService;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.S3Reader;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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

    Logger logger = LoggerFactory.getLogger(TransferController.class);

    @Autowired
    S3Reader s3Reader;

    @Autowired
    CrudService crudService;


    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @Async
    public ResponseEntity<String> start(@RequestBody TransferJobRequest request) throws Exception {
        JobParameters parameters = translate(new JobParametersBuilder(), request);
        crudService.insertBeforeTransfer(request);
        logger.info(String.valueOf(request.getSource().getVfsSourceCredential().getEncryptedSecret()));
        logger.info(request.getSource().getParentInfo().getPath());
        logger.info(String.valueOf(request.getChunkSize()));
        jc.setRequest(request);
        jc.setChunkSize(request.getChunkSize());
        asyncJobLauncher.run(jc.concurrentJobDefinition(), parameters);
        return ResponseEntity.status(HttpStatus.OK).body("Your batch job has been submitted with \n ID: " + request.getJobId());
    }

    public JobParameters translate(JobParametersBuilder builder, TransferJobRequest request) {
        logger.info("Request received : "+request.toString());
        builder.addLong(TIME, System.currentTimeMillis());
        builder.addString(OWNER_ID, request.getOwnerId());
        builder.addString(PRIORITY, String.valueOf(request.getPriority()));
        builder.addString(CHUNK_SIZE, String.valueOf(request.getChunkSize()));
        if (CredentialGroup.ACCOUNT_CRED_TYPE.contains(request.getSource().getType())) {
            AccountEndpointCredential credential = request.getSource().getVfsSourceCredential();
            builder.addString(SOURCE_CREDENTIAL_ID, credential.getUsername());
            builder.addString(SOURCE_URI, credential.getUri());
            builder.addString(SOURCE_BASE_PATH, request.getSource().getParentInfo().getPath());
        } else if (CredentialGroup.OAUTH_CRED_TYPE.contains(request.getSource().getType())) {
            OAuthEndpointCredential oauthCred = request.getSource().getOauthSourceCredential();

        }else{
            return null;
        }
        if (CredentialGroup.ACCOUNT_CRED_TYPE.contains(request.getDestination().getType())) {
            AccountEndpointCredential credential = request.getDestination().getVfsDestCredential();
            builder.addString(DEST_CREDENTIAL_ID, credential.getUsername());
            builder.addString(DEST_URI, credential.getUri());
            builder.addString(DEST_BASE_PATH, request.getDestination().getParentInfo().getPath());
        } else if (CredentialGroup.OAUTH_CRED_TYPE.contains(request.getDestination().getType())) {
            OAuthEndpointCredential oauthCred = request.getDestination().getOauthDestCredential();
        }else{
            return null;
        }
        return builder.toJobParameters();
    }
}

