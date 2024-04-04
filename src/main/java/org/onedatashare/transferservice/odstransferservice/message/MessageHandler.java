package org.onedatashare.transferservice.odstransferservice.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.amqp.core.Message;

public interface MessageHandler {
    void messageHandler(Message message) throws JsonProcessingException;
}
