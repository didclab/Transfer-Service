//package org.onedatashare.transferservice.odstransferservice.service;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.core.HazelcastJsonValue;
//import com.hazelcast.map.IMap;
//import jakarta.annotation.PostConstruct;
//import org.onedatashare.transferservice.odstransferservice.model.FileTransferNodeMetaData;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.core.env.Environment;
//import org.springframework.stereotype.Service;
//
//import java.util.UUID;
//
//@Service
//public class FileTransferNodeRegistrationLifeCycleListener {
//
//    private final HazelcastInstance hazelcastInstance;
//    private final ObjectMapper objectMapper;
//    private final IMap<String, HazelcastJsonValue> fileTransferNodeMap;
//    private final String appName;
//    private final String odsOwner;
//    private final UUID nodeUuid;
//    Logger logger = LoggerFactory.getLogger(FileTransferNodeRegistrationService.class);
//
//    public FileTransferNodeRegistrationLifeCycleListener(HazelcastInstance hazelcastInstance, Environment environment, ObjectMapper objectMapper) {
//        this.hazelcastInstance = hazelcastInstance;
//        this.appName = environment.getProperty("spring.application.name");
//        this.odsOwner = environment.getProperty("ods.user");
//        this.objectMapper = objectMapper;
//        this.nodeUuid = hazelcastInstance.getLocalEndpoint().getUuid();
//        this.fileTransferNodeMap = hazelcastInstance.getMap("file-transfer-node-map");
//    }
//
//    @PostConstruct
//    public void postConstruct() {
//        FileTransferNodeMetaData fileTransferNodeMetaData = FileTransferNodeMetaData.builder()
//                .nodeUuid(this.hazelcastInstance.getLocalEndpoint().getUuid())
//                .online(true)
//                .nodeName(this.appName)
//                .odsOwner(this.odsOwner)
//                .nodeUuid(this.nodeUuid)
//                .runningJob(false)
//                .jobUuid(new UUID(0, 0))
//                .jobId(-1)
//                .build();
//        try {
//            String json = this.objectMapper.writeValueAsString(fileTransferNodeMetaData);
//            logger.info("Registering client: {}", fileTransferNodeMetaData);
//            this.fileTransferNodeMap.put(this.appName, new HazelcastJsonValue(json));
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }
//    }
//}
