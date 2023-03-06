package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.onedatashare.transferservice.odstransferservice.model.DeadLetterQueueData;
import org.springframework.amqp.core.Message;
import org.springframework.batch.core.JobParameters;

import java.util.ArrayList;
import java.util.List;

public class DeadLetterQueueService {

    public DeadLetterQueueData getDataFromMesaage(Message message,  JsonProcessingException exception){
        DeadLetterQueueData data = new DeadLetterQueueData();
        data.failureException = new ArrayList<>();
        data.failureException.add(exception);
        return data;
    }

    public DeadLetterQueueData getDataFromJobExecution(JobParameters jobParameters, List<Throwable> failureExceptions){
        DeadLetterQueueData data = new DeadLetterQueueData();
        data.failureException = new ArrayList<>(failureExceptions);
        return data;
    }


}
