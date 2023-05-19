package org.onedatashare.transferservice.odstransferservice.consumer;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.TransferApplicationParams;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.onedatashare.transferservice.odstransferservice.service.VfsExpander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class RabbitMQConsumer {

    private final ObjectMapper objectMapper;
    private final ThreadPoolManager threadPoolManager;
    Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);

    JobControl jc;

    JobLauncher asyncJobLauncher;

    JobParamService jobParamService;

    Queue userQueue;

    VfsExpander vfsExpander;

    @Autowired
    Set<Long> jobIds;

    public RabbitMQConsumer(VfsExpander vfsExpander, Queue userQueue, JobParamService jobParamService, JobLauncher asyncJobLauncher, JobControl jc, ThreadPoolManager threadPoolManager) {
        this.vfsExpander = vfsExpander;
        this.userQueue = userQueue;
        this.jobParamService = jobParamService;
        this.asyncJobLauncher = asyncJobLauncher;
        this.jc = jc;
        this.threadPoolManager = threadPoolManager;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.ALWAYS);
    }

    @RabbitListener(queues = "#{userQueue}")
    public void consumeDefaultMessage(final Message message) {
        String jsonStr = new String(message.getBody());
        logger.info("Message recv: {}", jsonStr);
        try {
            TransferJobRequest request = objectMapper.readValue(jsonStr, TransferJobRequest.class);
            logger.info("Job Recieved: {}", request.toString());
            if (request.getSource().getType().equals(EndpointType.vfs)) {
                List<EntityInfo> fileExpandedList = vfsExpander.expandDirectory(request.getSource().getInfoList(), request.getSource().getParentInfo().getPath(), request.getChunkSize());
                request.getSource().setInfoList(new ArrayList<>(fileExpandedList));
            }
            JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
            jc.setRequest(request);
            JobExecution jobExecution = asyncJobLauncher.run(jc.concurrentJobDefinition(), parameters);
            this.jobIds.add(jobExecution.getJobId());
            return;
        } catch (Exception e) {
            logger.debug("Failed to parse jsonStr:{} to TransferJobRequest.java", jsonStr);
        }
        try {
            TransferApplicationParams params = objectMapper.readValue(jsonStr, TransferApplicationParams.class);
            logger.info("Parsed TransferApplicationParams:{}", params);
            this.threadPoolManager.applyOptimizer(params.getConcurrency(), params.getParallelism());
        } catch (Exception e) {
            logger.info("Did not apply transfer params due to parsing message failure");
        }
    }
}