package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

@Data
public class DeadLetterQueueData implements Serializable {
   public List <Throwable> failureException;
   public JobParameters jobParameters;
   public Collection<StepExecution> stepExecutions;
}

