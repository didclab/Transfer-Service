package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastJsonValue;
import org.onedatashare.transferservice.odstransferservice.message.MessageHandlerRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class HazelcastConsumer {

    private final IQueue<HazelcastJsonValue> messageQueue;
    private final ObjectMapper objectMapper;
    private final Logger logger;
    private final MessageHandlerRouter messageHandlerRouter;


    public HazelcastConsumer(IQueue<HazelcastJsonValue> messageQueue, ObjectMapper objectMapper, MessageHandlerRouter messageHandlerRouter) {
        this.messageQueue = messageQueue;
        this.objectMapper = objectMapper;
        this.messageHandlerRouter = messageHandlerRouter;
        this.logger = LoggerFactory.getLogger(HazelcastConsumer.class);
    }

    @Scheduled(cron = "0/10 * * * * *")
    public void consumer() {
        HazelcastJsonValue jsonMsg = null;
        try {
            jsonMsg = this.messageQueue.take();
        } catch (InterruptedException e) {
            return;
        }
        JsonNode jsonNode = null;
        try {
            jsonNode = this.objectMapper.readTree(jsonMsg.getValue());
        } catch (JsonProcessingException e) {
            logger.error("Failed to read json in Hazelcast Consumer: value={}: \n Error: {}", jsonMsg.getValue(), e.getMessage());
            return;
        }
        String type = this.messageHandlerRouter.getAndRemoveTypeFromMessage(jsonNode);
        HazelcastJsonValue properJsonMsg = new HazelcastJsonValue(jsonNode.toString());
        logger.info("Received Message Type: {} \n Json Message: {}", type, properJsonMsg.getValue());
        this.messageHandlerRouter.processMessage(properJsonMsg, type);
    }
}
