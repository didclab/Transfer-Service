package org.onedatashare.transferservice.odstransferservice.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class TransferJobRequestHandler implements MessageHandler {

    private final ObjectMapper objectMapper;
    private final JobParamService jobParamService;
    private final JobLauncher jobLauncher;
    private final JobControl jobControl;

    Logger logger = LoggerFactory.getLogger(TransferJobRequestHandler.class);

    public TransferJobRequestHandler(ObjectMapper messageObjectMapper, JobParamService jobParamService, JobLauncher jobLauncher, JobControl jobControl) {
        this.objectMapper = messageObjectMapper;
        this.jobParamService = jobParamService;
        this.jobLauncher = jobLauncher;
        this.jobControl = jobControl;
    }

    @Override
    public void messageHandler(Message message) throws IOException {
        String jsonStr = new String(message.getBody());
        TransferJobRequest request = objectMapper.readValue(jsonStr, TransferJobRequest.class);
        logger.info("Job Received: {}", request.toString());
        JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
        try {
            jobLauncher.run(jobControl.concurrentJobDefinition(request), parameters);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
