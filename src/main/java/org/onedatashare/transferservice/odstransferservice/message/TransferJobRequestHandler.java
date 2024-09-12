package org.onedatashare.transferservice.odstransferservice.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.onedatashare.transferservice.odstransferservice.service.expanders.ExpanderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class TransferJobRequestHandler implements MessageHandler {

    private final ObjectMapper objectMapper;
    private final JobControl jobControl;
    private final ExpanderFactory expanderFactory;


    Logger logger = LoggerFactory.getLogger(TransferJobRequestHandler.class);

    public TransferJobRequestHandler(ObjectMapper messageObjectMapper, JobControl jobControl, ExpanderFactory expanderFactory) {
        this.objectMapper = messageObjectMapper;
        this.jobControl = jobControl;
        this.expanderFactory = expanderFactory;
    }

    @Override
    public void messageHandler(Message message) throws IOException {
        String jsonStr = new String(message.getBody());
        TransferJobRequest request = objectMapper.readValue(jsonStr, TransferJobRequest.class);
        logger.info("Job Received: {}", request.toString());
        List<EntityInfo> fileInfo = expanderFactory.getExpander(request.getSource());
        request.getSource().setInfoList(fileInfo);
        try {
            this.jobControl.runJob(request);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
