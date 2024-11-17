package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.map.IMap;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.FileTransferNodeMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class FileTransferNodeRegistrationService implements LifecycleListener {

    private final IMap<String, HazelcastJsonValue> fileTransferNodeRegistrationMap;
    private final UUID nodeUuid;
    private final String appName;
    private final String odsOwner;
    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(FileTransferNodeRegistrationService.class);
    @Autowired
    JobControl jobControl;

    public FileTransferNodeRegistrationService(HazelcastInstance hazelcastInstance, IMap<String, HazelcastJsonValue> fileTransferNodeRegistrationMap, Environment environment, ObjectMapper objectMapper) {
        this.fileTransferNodeRegistrationMap = fileTransferNodeRegistrationMap;
        this.nodeUuid = hazelcastInstance.getLocalEndpoint().getUuid();
        hazelcastInstance.getLifecycleService().addLifecycleListener(this);
        this.appName = environment.getProperty("spring.application.name");
        this.odsOwner = environment.getProperty("ods.user");
        this.objectMapper = objectMapper;
    }

    public void updateRegistrationInHazelcast(JobExecution jobExecution) throws JsonProcessingException {
        var metaDataBuilder = FileTransferNodeMetaData.builder();
        if (jobExecution == null) {
            metaDataBuilder.jobId(-1L);
            metaDataBuilder.runningJob(false);
            metaDataBuilder.jobUuid(new UUID(0, 0));
        } else {
            metaDataBuilder.jobId(jobExecution.getJobId());
            metaDataBuilder.runningJob(jobExecution.isRunning());
            metaDataBuilder.jobUuid(UUID.fromString(jobExecution.getJobParameters().getString(ODSConstants.JOB_UUID)));
        }
        metaDataBuilder.online(true);
        metaDataBuilder.nodeName(this.appName);
        metaDataBuilder.odsOwner(this.odsOwner);
        metaDataBuilder.nodeUuid(this.nodeUuid);
        String jsonValue = this.objectMapper.writeValueAsString(metaDataBuilder.build());
        logger.info("Registering node: {}", jsonValue);
        this.fileTransferNodeRegistrationMap.put(this.appName, new HazelcastJsonValue(jsonValue));
    }

    @Override
    public void stateChanged(LifecycleEvent lifecycleEvent) {
        if (lifecycleEvent.getState().equals(LifecycleEvent.LifecycleState.CLIENT_CONNECTED)) {
            try {
                this.updateRegistrationInHazelcast(this.jobControl.latestJobExecution);
            } catch (JsonProcessingException e) {
                logger.error("Error while updating registration when client connected to cluster");
            }
        }
    }
}
