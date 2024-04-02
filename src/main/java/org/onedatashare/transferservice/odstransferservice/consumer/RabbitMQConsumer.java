package org.onedatashare.transferservice.odstransferservice.consumer;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.onedatashare.transferservice.odstransferservice.Enum.MessageType;
import org.onedatashare.transferservice.odstransferservice.model.CarbonMeasureRequest;
import org.onedatashare.transferservice.odstransferservice.model.CarbonMeasureResponse;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.metrics.CarbonScore;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.TransferApplicationParams;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolContract;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.onedatashare.transferservice.odstransferservice.service.PmeterParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

import static org.springframework.amqp.core.MessageProperties.CONTENT_TYPE_JSON;

@Service
public class RabbitMQConsumer {

    private final ObjectMapper objectMapper;
    private final ThreadPoolContract threadPool;
    private final PmeterParser pmeterParser;
    Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);

    @Value("${spring.application.name}")
    String applicationName;

    JobControl jc;

    JobLauncher jobLauncher;

    JobParamService jobParamService;

    Queue userQueue;

    @Autowired
    RabbitTemplate rabbitTemplate;


    public RabbitMQConsumer(Queue userQueue, JobParamService jobParamService, JobLauncher asyncJobLauncher, JobControl jc, ThreadPoolContract threadPool, PmeterParser pmeterParser) {
        this.userQueue = userQueue;
        this.jobParamService = jobParamService;
        this.jobLauncher = asyncJobLauncher;
        this.jc = jc;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        this.objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.ALWAYS);
        this.threadPool = threadPool;
        this.pmeterParser = pmeterParser;
    }

    @RabbitListener(queues = "#{userQueue}")
    public void consumeDefaultMessage(Message message) throws JsonProcessingException {
        String jsonStr = new String(message.getBody());
        MessageType messageType = MessageType.valueOf(message.getMessageProperties().getHeader("type"));
        switch (messageType) {

            case TRANSFER_JOB_REQUEST: {
                TransferJobRequest request = objectMapper.readValue(jsonStr, TransferJobRequest.class);
                logger.info("Job Received: {}", request.toString());
                JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
                try {
                    jc.setRequest(request);
                    jobLauncher.run(jc.concurrentJobDefinition(), parameters);
                    return;
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }

            case APPLICATION_PARAM_CHANGE: {
                TransferApplicationParams params = objectMapper.readValue(jsonStr, TransferApplicationParams.class);
                logger.info("Parsed TransferApplicationParams: {}", params);
                this.threadPool.applyOptimizer(params.getConcurrency(), params.getParallelism());
            }

            case CARBON_AVG_REQUEST: {
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
                MessagePostProcessor messagePostProcessor = this.embedMessageType(message.getMessageProperties().getCorrelationId());
                Message msg = MessageBuilder.withBody(jsonResp.getBytes())
                        .setContentType(MediaType.APPLICATION_JSON_VALUE)
                        .build();
                this.rabbitTemplate.convertAndSend(message.getMessageProperties().getReplyTo(), msg, messagePostProcessor);
            }

            case CARBON_IP_REQUEST: {
                CarbonMeasureRequest carbonMeasureRequest = objectMapper.readValue(jsonStr, CarbonMeasureRequest.class);
                logger.info("Received CarbonMeasureRequest: {}", carbonMeasureRequest);
                Map<String, Object> sourceTraceRouteCarbon = this.pmeterParser.carbonPerIp(carbonMeasureRequest.sourceIp);
                Map<String, Object> destinationTraceRouteCarbon = this.pmeterParser.carbonPerIp(carbonMeasureRequest.destinationIp);
                Map<String, Object> mergedMap = new HashMap<>();
                mergedMap.putAll(sourceTraceRouteCarbon);
                mergedMap.putAll(destinationTraceRouteCarbon);
                String jsonResp = this.objectMapper.writeValueAsString(mergedMap);
                MessagePostProcessor messagePostProcessor = this.embedMessageType(message.getMessageProperties().getCorrelationId());
                Message msg = MessageBuilder.withBody(jsonResp.getBytes())
                        .setContentType(MediaType.APPLICATION_JSON_VALUE)
                        .build();
                this.rabbitTemplate.convertAndSend(message.getMessageProperties().getReplyTo(), msg, messagePostProcessor);

            }
        }
    }

    public MessagePostProcessor embedMessageType(String correlationId) {
        return message -> {
            message.getMessageProperties().setCorrelationId(correlationId);
            message.getMessageProperties().setType(CONTENT_TYPE_JSON);
            return message;
        };
    }

}