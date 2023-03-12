package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.onedatashare.transferservice.odstransferservice.model.DeadLetterQueueData;
import org.springframework.amqp.core.Message;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Service
public class DeadLetterQueueService {

    public DeadLetterQueueData convertDataToDLQ(JobParameters jobParameters, List<Throwable> failureExceptions, Collection<StepExecution> stepExecutions){
        DeadLetterQueueData data = new DeadLetterQueueData();
        data.failureException = new ArrayList<>(failureExceptions);
        data.jobParameters = jobParameters;
        data.stepExecutions = new ArrayList<>(stepExecutions);
        return data;
    }

}
