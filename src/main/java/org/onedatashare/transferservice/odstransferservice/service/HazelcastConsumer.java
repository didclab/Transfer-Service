package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastJsonValue;
import jakarta.annotation.PostConstruct;
import org.onedatashare.transferservice.odstransferservice.Enum.MessageType;
import org.onedatashare.transferservice.odstransferservice.message.TransferApplicationParamHandler;
import org.onedatashare.transferservice.odstransferservice.message.TransferJobRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class HazelcastConsumer implements Runnable {

    private final IQueue<HazelcastJsonValue> messageQueue;
    private final ObjectMapper objectMapper;
    private final TransferJobRequestHandler transferJobRequestHandler;
    private final TransferApplicationParamHandler transferParamApplicationHandler;
    private final Logger logger;
    private Thread consumerThread;

    public HazelcastConsumer(IQueue<HazelcastJsonValue> messageQueue, ObjectMapper objectMapper, TransferJobRequestHandler transferJobRequestHandler, TransferApplicationParamHandler transferApplicationParamHandler) {
        this.messageQueue = messageQueue;
        this.transferJobRequestHandler = transferJobRequestHandler;
        this.objectMapper = objectMapper;
        this.transferParamApplicationHandler = transferApplicationParamHandler;
        this.logger = LoggerFactory.getLogger(HazelcastConsumer.class);
        this.consumerThread = new Thread(this);
    }

    @PostConstruct
    public void init() {
        this.consumerThread.start();
    }


    @Override
    public void run() {
        while (true) {
            try {
                HazelcastJsonValue jsonMsg = this.messageQueue.take();
                JsonNode jsonNode = this.objectMapper.readTree(jsonMsg.getValue());
                String type = ((ObjectNode) jsonNode).get("type").toString();
                ((ObjectNode) jsonNode).remove("type");
                HazelcastJsonValue properJsonMsg = new HazelcastJsonValue(jsonNode.toString());
                logger.info("Received message: {}", properJsonMsg);
                switch (MessageType.valueOf(type)) {
                    case TRANSFER_JOB_REQUEST:
                        this.transferJobRequestHandler.messageHandler(properJsonMsg);
                        break;

                    case APPLICATION_PARAM_CHANGE:
                        this.transferParamApplicationHandler.messageHandler(properJsonMsg);
                }
            } catch (InterruptedException | JsonProcessingException e) {
                logger.error(e.getMessage());
            }

        }
    }
}
