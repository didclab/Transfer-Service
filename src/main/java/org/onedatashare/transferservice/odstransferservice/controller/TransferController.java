package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.StaticVar;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.CredentialGroup;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.CrudService;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    Logger logger = LoggerFactory.getLogger(TransferController.class);

    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @Async
    public ResponseEntity<String> start(@RequestBody TransferJobRequest request) throws Exception {
        JobParameters parameters = translate(new JobParametersBuilder(), request);
        Map<String, Long> hm = new HashMap<>();
        //TODO: Must remove these and find way to pass to reader and writer
        crudService.insertBeforeTransfer(request);
        for (EntityInfo ei : request.getSource().getInfoList()) {
            System.out.println(ei.toString());
            hm.put(ei.getPath(), ei.getSize());
        }
        StaticVar.setHm(hm);
        jc.setRequest(request);
        jc.setChunkSize(request.getChunkSize());
        logger.info(String.valueOf(request.getChunkSize()));
        asyncJobLauncher.run(jc.concurrentJobDefinition(), parameters);
        return ResponseEntity.status(HttpStatus.OK).body("Your batch job has been submitted with \n ID: " + request.getId());
    }

    @Autowired
    CrudService crudService;

    public JobParameters translate(JobParametersBuilder builder, TransferJobRequest request) {
        System.out.println(request.toString());
        builder.addLong(TIME, System.currentTimeMillis());
        builder.addString(OWNER_ID, request.getOwnerId());
        builder.addString(PRIORITY, String.valueOf(request.getPriority()));
        builder.addString(CHUNK_SIZE, String.valueOf(request.getChunkSize()));
        if(CredentialGroup.ACCOUNT_CRED_TYPE.contains(request.getSource().getType())){
            AccountEndpointCredential credential = request.getSource().getVfsSource();
            builder.addString(SOURCE_CREDENTIAL_ID, credential.getUsername());
            //builder.addString(SOURCE_ACCOUNT_ID_PASS, credential.getSecret() + ":" + "xxx");// request.getSource().getCredential().getPassword());
            builder.addString(SOURCE_URI, credential.getUri());
            builder.addString(SOURCE_BASE_PATH, request.getSource().getInfo().getPath());
            StaticVar.setVfsSource(credential);
        }else if(CredentialGroup.OAUTH_CRED_TYPE.contains(request.getSource().getType())){
            OAuthEndpointCredential oauthCred = request.getSource().getOauthSource();
            StaticVar.setOauthSource(oauthCred);
        }
        if(CredentialGroup.ACCOUNT_CRED_TYPE.contains(request.getDestination().getType())){
            AccountEndpointCredential credential = request.getDestination().getVfsDest();
            StaticVar.setVfsDest(credential);
            builder.addString(DEST_CREDENTIAL_ID, credential.getUsername());
            builder.addString(DEST_URI, credential.getUri());
            //builder.addString(DESTINATION_ACCOUNT_ID_PASS, credential.getSecret() + ":" + "xxx");// request.getDestination().getCredential().getPassword());
            builder.addString(DEST_BASE_PATH, request.getDestination().getInfo().getPath());
        }else if(CredentialGroup.OAUTH_CRED_TYPE.contains(request.getDestination().getType())){
            OAuthEndpointCredential oauthCred = request.getDestination().getOauthDest();
            StaticVar.setOauthDest(oauthCred);
        }
        return builder.toJobParameters();
    }
}

