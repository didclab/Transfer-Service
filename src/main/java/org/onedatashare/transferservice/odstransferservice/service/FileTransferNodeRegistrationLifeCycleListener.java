package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.map.IMap;
import org.onedatashare.transferservice.odstransferservice.model.FileTransferNodeMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import java.util.UUID;

public class FileTransferNodeRegistrationLifeCycleListener implements LifecycleListener {

    private final HazelcastInstance hazelcastInstance;
    private final ObjectMapper objectMapper;
    private final IMap<String, HazelcastJsonValue> fileTransferNodeMap;
    private final String appName;
    private final String odsOwner;
    private final UUID nodeUuid;
    Logger logger = LoggerFactory.getLogger(FileTransferNodeRegistrationService.class);

    public FileTransferNodeRegistrationLifeCycleListener(HazelcastInstance hazelcastInstance, Environment environment, ObjectMapper objectMapper) {
        this.hazelcastInstance = hazelcastInstance;
        this.appName = environment.getProperty("spring.application.name");
        this.odsOwner = environment.getProperty("ods.user");
        this.objectMapper = objectMapper;
        this.nodeUuid = hazelcastInstance.getLocalEndpoint().getUuid();
        this.fileTransferNodeMap = hazelcastInstance.getMap("file-transfer-node-map");
    }

    @Override
    public void stateChanged(LifecycleEvent event) {
        if (event.getState() == LifecycleEvent.LifecycleState.CLIENT_CONNECTED) {
            FileTransferNodeMetaData fileTransferNodeMetaData = FileTransferNodeMetaData.builder()
                    .nodeUuid(this.hazelcastInstance.getLocalEndpoint().getUuid())
                    .online(true)
                    .nodeName(this.appName)
                    .odsOwner(this.odsOwner)
                    .nodeUuid(this.nodeUuid)
                    .runningJob(false)
                    .jobUuid(new UUID(0, 0))
                    .jobId(-1)
                    .build();
            try {
                String json = this.objectMapper.writeValueAsString(fileTransferNodeMetaData);
                logger.info("Registering client: {}", fileTransferNodeMetaData);
                this.fileTransferNodeMap.put(this.appName, new HazelcastJsonValue(json));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        if (event.getState() == LifecycleEvent.LifecycleState.SHUTTING_DOWN) {
            try {
                String jsonValue = this.fileTransferNodeMap.get(this.nodeUuid).getValue();
                FileTransferNodeMetaData fileTransferNodeMetaData = this.objectMapper.readValue(jsonValue, FileTransferNodeMetaData.class);
                fileTransferNodeMetaData.setRunningJob(false);
                fileTransferNodeMetaData.setOnline(false);
                logger.info("De-Registering client: {}",fileTransferNodeMetaData);
                jsonValue = this.objectMapper.writeValueAsString(fileTransferNodeMetaData);
                this.fileTransferNodeMap.put(this.appName, new HazelcastJsonValue(jsonValue));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }


    }
}
