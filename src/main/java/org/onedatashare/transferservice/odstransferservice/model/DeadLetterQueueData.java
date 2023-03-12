package org.onedatashare.transferservice.odstransferservice.model;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public class DeadLetterQueueData implements Serializable {
   public List <Throwable> failureException;

   public JobParameters jobParameters;

   public String failureString;

   public Collection<StepExecution> stepExecutions;

   public JobParameters getJobParameters() {
      return jobParameters;
   }

   public void setJobParameters(JobParameters jobParameters) {
      this.jobParameters = jobParameters;
   }

   public List<Throwable> getFailureException() {
      return failureException;
   }

   public void setFailureException(List<Throwable> failureException) {
      this.failureException = failureException;
   }

   public Collection<StepExecution> getStepExecutions() {
      return stepExecutions;
   }

   public void setStepExecutions(Collection<StepExecution> stepExecutions) {
      this.stepExecutions = stepExecutions;
   }
}

