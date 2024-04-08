package org.onedatashare.transferservice.odstransferservice.message;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.onedatashare.transferservice.odstransferservice.consumer.RabbitMQConsumer;
import org.onedatashare.transferservice.odstransferservice.model.CarbonMeasureRequest;
import org.onedatashare.transferservice.odstransferservice.model.CarbonMeasureResponse;
import org.onedatashare.transferservice.odstransferservice.model.metrics.CarbonScore;
import org.onedatashare.transferservice.odstransferservice.service.PmeterParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class CarbonAvgRequestHandler implements MessageHandler {

    private final ObjectMapper objectMapper;
    private final PmeterParser pmeterParser;
    private final RabbitTemplate rabbitTemplate;

    @Value("${spring.application.name}")
    String applicationName;


    Logger logger = LoggerFactory.getLogger(CarbonAvgRequestHandler.class);

    public CarbonAvgRequestHandler(ObjectMapper messageObjectMapper, PmeterParser pmeterParser, RabbitTemplate rabbitTemplate) {
        this.objectMapper = messageObjectMapper;
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        this.objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.ALWAYS);
        this.pmeterParser = pmeterParser;
        this.rabbitTemplate = rabbitTemplate;
    }

    @Override
    public void messageHandler(Message message) throws IOException {
        String jsonStr = new String(message.getBody());
        CarbonMeasureRequest carbonMeasureRequest = objectMapper.readValue(jsonStr, CarbonMeasureRequest.class);
        logger.info("Received CarbonMeasureRequest: {}", carbonMeasureRequest);
        CarbonScore sourceCarbonScore = this.pmeterParser.carbonAverageTraceRoute(carbonMeasureRequest.sourceIp);
        CarbonScore destCarbonScore = this.pmeterParser.carbonAverageTraceRoute(carbonMeasureRequest.destinationIp);
        double average = (double) (sourceCarbonScore.getAvgCarbon() + destCarbonScore.getAvgCarbon()) / 2;
        CarbonMeasureResponse resp = new CarbonMeasureResponse();
        resp.transferNodeName = this.applicationName;
        resp.averageCarbonIntensity = average;
        logger.info("Response: CarbonMeasureResponse: {}", resp);
        String jsonResp = this.objectMapper.writeValueAsString(resp);
        MessagePostProcessor messagePostProcessor = RabbitMQConsumer.embedMessageType(message.getMessageProperties().getCorrelationId());
        Message msg = MessageBuilder.withBody(jsonResp.getBytes())
                .setContentType(MediaType.APPLICATION_JSON_VALUE)
                .build();
        this.rabbitTemplate.convertAndSend(message.getMessageProperties().getReplyTo(), msg, messagePostProcessor);

    }
}
