package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.CredentialGroup;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.stereotype.Service;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

@Service
public class JobParamService {

    Logger logger = LoggerFactory.getLogger(JobParamService.class);

    public JobParameters translate(JobParametersBuilder builder, TransferJobRequest request) {
        logger.info("Setting job Parameters");
        builder.addLong(TIME, System.currentTimeMillis());
        builder.addString(OWNER_ID, request.getOwnerId());
        builder.addString(PRIORITY, String.valueOf(request.getPriority()));
        builder.addString(CHUNK_SIZE, String.valueOf(request.getChunkSize()));
        builder.addString(SOURCE_BASE_PATH, request.getSource().getParentInfo().getPath());
        builder.addString(DEST_BASE_PATH, request.getDestination().getParentInfo().getPath());
        if (CredentialGroup.ACCOUNT_CRED_TYPE.contains(request.getSource().getType())) {
            AccountEndpointCredential credential = request.getSource().getVfsSourceCredential();
            builder.addString(SOURCE_CREDENTIAL_ID, credential.getUsername());
            builder.addString(SOURCE_URI, credential.getUri());
        } else if (CredentialGroup.OAUTH_CRED_TYPE.contains(request.getSource().getType())) {
            OAuthEndpointCredential oauthCred = request.getSource().getOauthSourceCredential();
        }
        if (CredentialGroup.ACCOUNT_CRED_TYPE.contains(request.getDestination().getType())) {
            AccountEndpointCredential credential = request.getDestination().getVfsDestCredential();
            builder.addString(DEST_CREDENTIAL_ID, credential.getUsername());
            builder.addString(DEST_URI, credential.getUri());
        } else if (CredentialGroup.OAUTH_CRED_TYPE.contains(request.getDestination().getType())) {
            OAuthEndpointCredential oauthCred = request.getDestination().getOauthDestCredential();
        }
        return builder.toJobParameters();
    }
}
