package org.onedatashare.transferservice.odstransferservice.consumer;


import com.google.gson.Gson;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.CrudService;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class RabbitMQConsumer {

    Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);

    @Autowired
    JobControl jc;

    @Autowired
    JobLauncher asyncJobLauncher;

    @Autowired
    JobParamService jobParamService;

    @Autowired
    CrudService crudService;

    @Autowired
    Queue userQueue;

    @RabbitListener(queues = "#{userQueue}")
    public void consumeDefaultMessage(final Message message) {
        String jsonStr = new String(message.getBody());
        System.out.println("Message received");// + jsonStr);

        Gson g = new Gson();
        TransferJobRequest request = g.fromJson(jsonStr, TransferJobRequest.class);
        logger.info(request.toString());
        try {
            JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
            crudService.insertBeforeTransfer(request);
            jc.setRequest(request);
            asyncJobLauncher.run(jc.concurrentJobDefinition(), parameters);
        } catch (Exception e) {
            logger.error("Not able to start job");
            e.printStackTrace();
        }
    }
}