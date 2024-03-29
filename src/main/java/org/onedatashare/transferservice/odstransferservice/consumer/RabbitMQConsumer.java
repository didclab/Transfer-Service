package org.onedatashare.transferservice.odstransferservice.consumer;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.TransferApplicationParams;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolContract;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.onedatashare.transferservice.odstransferservice.service.expanders.VfsExpander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class RabbitMQConsumer {

    private final ObjectMapper objectMapper;
    private final ThreadPoolContract threadPool;
    Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);

    JobControl jc;

    JobLauncher jobLauncher;

    JobParamService jobParamService;

    Queue userQueue;


    public RabbitMQConsumer(Queue userQueue, JobParamService jobParamService, JobLauncher asyncJobLauncher, JobControl jc, ThreadPoolContract threadPool) {
        this.userQueue = userQueue;
        this.jobParamService = jobParamService;
        this.jobLauncher = asyncJobLauncher;
        this.jc = jc;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        this.objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.ALWAYS);
        this.threadPool = threadPool;
    }

    @RabbitListener(queues = "#{userQueue}")
    public void consumeDefaultMessage(final Message message) {
        String jsonStr = new String(message.getBody());

        logger.info("Message recv: {}", jsonStr);
        try {
            TransferJobRequest request = objectMapper.readValue(jsonStr, TransferJobRequest.class);
            logger.info("Job Recieved: {}", request.toString());

            JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
            jc.setRequest(request);
            jobLauncher.run(jc.concurrentJobDefinition(), parameters);

            return;
        } catch (Exception e) {
            logger.error("Failed to parse jsonStr: {} to TransferJobRequest.java", jsonStr);
        }
        try {
            TransferApplicationParams params = objectMapper.readValue(jsonStr, TransferApplicationParams.class);
            logger.info("Parsed TransferApplicationParams: {}", params);
            this.threadPool.applyOptimizer(params.getConcurrency(), params.getParallelism());
        } catch (Exception e) {
            logger.error("Did not apply transfer params due to parsing message failure");
        }
    }
}