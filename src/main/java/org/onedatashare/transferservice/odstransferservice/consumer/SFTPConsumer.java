package org.onedatashare.transferservice.odstransferservice.consumer;

import com.google.gson.Gson;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.CrudService;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author deepika
 */
@Service
public class SFTPConsumer {

    Logger logger = LoggerFactory.getLogger(SFTPConsumer.class);

    @Autowired
    ConsumerUtility consumerUtility;

    @RabbitListener(bindings =
    @QueueBinding(exchange = @Exchange("${ods.rabbitmq.exchange}"),
            value = @Queue("${ods.rabbitmq.sftp.queue}"),
            key = "${ods.rabbitmq.sftp.queue}"))
    public void consumeDefaultMessage(final Message message) throws Exception {
        String jsonStr = new String(message.getBody());
        System.out.println("SFTP message received");// + jsonStr);
        Gson g = new Gson();
        TransferJobRequest request = g.fromJson(jsonStr, TransferJobRequest.class);
        logger.info(request.toString());
        try {
            consumerUtility.process(request);
        } catch (Exception e) {
            logger.error("Not able to start job");
            e.printStackTrace();
        }
    }
}