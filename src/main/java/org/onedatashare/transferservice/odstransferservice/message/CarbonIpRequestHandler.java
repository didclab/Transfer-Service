package org.onedatashare.transferservice.odstransferservice.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.onedatashare.transferservice.odstransferservice.consumer.RabbitMQConsumer;
import org.onedatashare.transferservice.odstransferservice.model.CarbonMeasureRequest;
import org.onedatashare.transferservice.odstransferservice.service.PmeterParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class CarbonIpRequestHandler implements MessageHandler {

    private final ObjectMapper objectMapper;
    private final PmeterParser pmeterParser;
    private final RabbitTemplate rabbitTemplate;

    Logger logger = LoggerFactory.getLogger(CarbonIpRequestHandler.class);

    public CarbonIpRequestHandler(ObjectMapper messageObjectMapper, PmeterParser pmeterParser, RabbitTemplate rabbitTemplate) {
        this.objectMapper = messageObjectMapper;
        this.pmeterParser = pmeterParser;
        this.rabbitTemplate = rabbitTemplate;

    }

    @Override
    public void messageHandler(Message message) throws JsonProcessingException {
        String jsonStr = new String(message.getBody());
        CarbonMeasureRequest carbonMeasureRequest = objectMapper.readValue(jsonStr, CarbonMeasureRequest.class);
        logger.info("Received CarbonMeasureRequest: {}", carbonMeasureRequest);
        Map<String, Object> sourceTraceRouteCarbon = this.pmeterParser.carbonPerIp(carbonMeasureRequest.sourceIp);
        Map<String, Object> destinationTraceRouteCarbon = this.pmeterParser.carbonPerIp(carbonMeasureRequest.destinationIp);
        Map<String, Object> mergedMap = new HashMap<>();
        mergedMap.putAll(sourceTraceRouteCarbon);
        mergedMap.putAll(destinationTraceRouteCarbon);
        String jsonResp = this.objectMapper.writeValueAsString(mergedMap);
        MessagePostProcessor messagePostProcessor = RabbitMQConsumer.embedMessageType(message.getMessageProperties().getCorrelationId());
        Message msg = MessageBuilder.withBody(jsonResp.getBytes())
                .setContentType(MediaType.APPLICATION_JSON_VALUE)
                .build();
        logger.info("Sending reply too: {}", message.getMessageProperties().getReplyTo());
        this.rabbitTemplate.convertAndSend(message.getMessageProperties().getReplyTo(), msg, messagePostProcessor);

    }
}
