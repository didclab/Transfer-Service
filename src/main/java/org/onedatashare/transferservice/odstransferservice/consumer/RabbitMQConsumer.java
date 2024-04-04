package org.onedatashare.transferservice.odstransferservice.consumer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.rabbitmq.client.Channel;
import org.onedatashare.transferservice.odstransferservice.Enum.MessageType;
import org.onedatashare.transferservice.odstransferservice.message.CarbonAvgRequestHandler;
import org.onedatashare.transferservice.odstransferservice.message.CarbonIpRequestHandler;
import org.onedatashare.transferservice.odstransferservice.message.TransferApplicationParamHandler;
import org.onedatashare.transferservice.odstransferservice.message.TransferJobRequestHandler;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;

import static org.springframework.amqp.core.MessageProperties.CONTENT_TYPE_JSON;

@Service
public class RabbitMQConsumer {

    private final TransferJobRequestHandler transferJobRequestHandler;
    private final CarbonAvgRequestHandler carbonAvgRequestHandler;
    private final TransferApplicationParamHandler transferApplicationParamHandler;

    private final CarbonIpRequestHandler carbonIpRequestHandler;

    Queue userQueue;

    RabbitTemplate rabbitTemplate;


    public RabbitMQConsumer(RabbitTemplate rabbitTemplate, Queue userQueue, TransferJobRequestHandler transferJobRequestHandler, CarbonAvgRequestHandler carbonAvgRequestHandler, TransferApplicationParamHandler transferApplicationParamHandler, CarbonIpRequestHandler carbonIpRequestHandler) {
        this.userQueue = userQueue;
        this.transferJobRequestHandler = transferJobRequestHandler;
        this.carbonAvgRequestHandler = carbonAvgRequestHandler;
        this.transferApplicationParamHandler = transferApplicationParamHandler;
        this.carbonIpRequestHandler = carbonIpRequestHandler;
        this.rabbitTemplate = rabbitTemplate;
    }

    @RabbitListener(queues = "#{userQueue}")
    public void consumeDefaultMessage(Message message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        MessageType messageType = MessageType.valueOf(message.getMessageProperties().getHeader("type"));
        switch (messageType) {
            case TRANSFER_JOB_REQUEST: {
                this.transferJobRequestHandler.messageHandler(message);
            }

            case APPLICATION_PARAM_CHANGE: {
                this.transferApplicationParamHandler.messageHandler(message);
            }

            case CARBON_AVG_REQUEST: {
                this.carbonAvgRequestHandler.messageHandler(message);
            }

            case CARBON_IP_REQUEST: {
                this.carbonIpRequestHandler.messageHandler(message);
            }
        }
        channel.basicAck(tag, false);
    }

    public static MessagePostProcessor embedMessageType(String correlationId) {
        return message -> {
            message.getMessageProperties().setCorrelationId(correlationId);
            message.getMessageProperties().setType(CONTENT_TYPE_JSON);
            return message;
        };
    }

}