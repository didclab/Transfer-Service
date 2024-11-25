package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastJsonValue;
import org.onedatashare.transferservice.odstransferservice.Enum.MessageType;
import org.onedatashare.transferservice.odstransferservice.message.StopJobRequestHandler;
import org.onedatashare.transferservice.odstransferservice.message.TransferApplicationParamHandler;
import org.onedatashare.transferservice.odstransferservice.message.TransferJobRequestHandler;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolContract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class HazelcastConsumer {

    private final IQueue<HazelcastJsonValue> messageQueue;
    private final ObjectMapper objectMapper;
    private final TransferJobRequestHandler transferJobRequestHandler;
    private final TransferApplicationParamHandler transferParamApplicationHandler;
    private final Logger logger;
    private final StopJobRequestHandler stopJobRequestHandler;
    private final TaskExecutor executor;

    public HazelcastConsumer(ThreadPoolContract threadPoolContract, StopJobRequestHandler stopJobRequestHandler, IQueue<HazelcastJsonValue> messageQueue, ObjectMapper objectMapper, TransferJobRequestHandler transferJobRequestHandler, TransferApplicationParamHandler transferApplicationParamHandler) {
        this.messageQueue = messageQueue;
        this.transferJobRequestHandler = transferJobRequestHandler;
        this.objectMapper = objectMapper;
        this.transferParamApplicationHandler = transferApplicationParamHandler;
        this.logger = LoggerFactory.getLogger(HazelcastConsumer.class);
        this.stopJobRequestHandler = stopJobRequestHandler;
        this.executor = threadPoolContract.createExecutor(-1, "HazelcastConsumer");
    }


    @Scheduled(cron = "0/5 * * * * *")
    public void runConsumer() throws JsonProcessingException {
        HazelcastJsonValue jsonMsg = this.messageQueue.poll();
        if (jsonMsg == null) return;
        JsonNode jsonNode = this.objectMapper.readTree(jsonMsg.getValue());
        logger.info("Got Msg: {}", jsonNode.toPrettyString());
        String type = ((ObjectNode) jsonNode).get("type").asText();
        ((ObjectNode) jsonNode).remove("type");
        HazelcastJsonValue properJsonMsg = new HazelcastJsonValue(jsonNode.toString());
        this.executor.execute(() -> {
            switch (MessageType.valueOf(type)) {
                case MessageType.TRANSFER_JOB_REQUEST:
                    try {
                        this.transferJobRequestHandler.messageHandler(properJsonMsg);
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to parse json in TransferJobReqeust Message Handler: {} \n Error: {}", properJsonMsg, e.getMessage());
                    }
                    break;

                case MessageType.APPLICATION_PARAM_CHANGE:
                    try {
                        this.transferParamApplicationHandler.messageHandler(properJsonMsg);
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to parse json in TransferParam  Message Handler: {} \n Error: {}", properJsonMsg, e.getMessage());
                    }
                    break;

                case MessageType.STOP_JOB_REQUEST:
                    try {
                        this.stopJobRequestHandler.messageHandler(properJsonMsg);
                    } catch (IOException e) {
                        logger.error("Failed to parse json in Stop Job Message Handler: {} \n Error: {}", properJsonMsg, e.getMessage());
                    }
                    break;
            }
        });
    }

}
