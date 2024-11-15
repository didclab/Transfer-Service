package org.onedatashare.transferservice.odstransferservice.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.core.HazelcastJsonValue;
import org.onedatashare.transferservice.odstransferservice.Enum.MessageType;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolContract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class MessageHandlerRouter {

    TransferApplicationParamHandler transferApplicationParamHandler;
    TransferJobRequestHandler transferJobRequestHandler;
    private final TaskExecutor executorService;
    private Logger logger;

    public MessageHandlerRouter(ThreadPoolContract threadPoolContract, TransferJobRequestHandler transferJobRequestHandler, TransferApplicationParamHandler transferApplicationParamHandler) {
        this.transferJobRequestHandler = transferJobRequestHandler;
        this.transferApplicationParamHandler = transferApplicationParamHandler;
        this.executorService = threadPoolContract.createExecutor(4, "hz-consumer");
        this.logger = LoggerFactory.getLogger(MessageHandlerRouter.class);
    }

    public void processMessage(HazelcastJsonValue properJsonMsg, String type) {
        switch (MessageType.valueOf(type)) {
            case TRANSFER_JOB_REQUEST:
                this.executorService.execute(() -> {
                    try {
                        this.transferJobRequestHandler.messageHandler(properJsonMsg);
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to read json in Hazelcast Consumer: value={}: \n Error: {}", properJsonMsg, e.getMessage());
                    }
                });
                break;

            case APPLICATION_PARAM_CHANGE:
                this.executorService.execute(() -> {
                    try {
                        this.transferApplicationParamHandler.messageHandler(properJsonMsg);
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to read json in Hazelcast Consumer: value={}: \n Error: {}", properJsonMsg, e.getMessage());
                    }
                });
                break;
        }
    }

    public String getAndRemoveTypeFromMessage(JsonNode jsonNode) {
        String type = ((ObjectNode) jsonNode).get("type").asText();
        ((ObjectNode) jsonNode).remove("type");
        return type;
    }


}
