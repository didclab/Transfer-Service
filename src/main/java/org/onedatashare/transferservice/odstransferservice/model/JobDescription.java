package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Getter;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Getter
public class JobDescription {
    private BatchStatus status;
    private Date startTime;
    private Date createTime;

    private Date endTime;

    private Date lastUpdated;

    private ExitStatus exitStatus;

    private String jobName;
    List<StepDescriptor> stepDescriptorList;

    public static JobDescription convertFromJobExecution(JobExecution jobExecution) {
        JobDescription jobDescription = new JobDescription();
        Date startTime = jobExecution.getStartTime();
        if (startTime != null) {
            jobDescription.startTime = startTime;
        }
        Date endTime = jobExecution.getEndTime();
        if (endTime != null) {
            jobDescription.endTime = endTime;
        }
        jobDescription.status = jobExecution.getStatus();
        Date lastUpdated = jobExecution.getLastUpdated();
        if (lastUpdated != null) {
            jobDescription.lastUpdated = lastUpdated;
        }
        jobDescription.exitStatus = jobExecution.getExitStatus();
        jobDescription.jobName = jobExecution.getJobInstance().getJobName();
        jobDescription.createTime = jobExecution.getCreateTime();
        List<StepDescriptor> stepDescriptorList = new ArrayList<>();
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            stepDescriptorList.add(StepDescriptor.convertFromStepExecution(stepExecution));
        }
        jobDescription.stepDescriptorList = stepDescriptorList;
        return jobDescription;
    }
}

@Getter
class StepDescriptor {
    private String stepName;

    private BatchStatus status;

    private int readCount;

    private int writeCount;

    private int commitCount;

    private Date startTime;

    private Date endTime;

    private Date lastUpdated;

    private ExitStatus exitStatus;

    private List<Throwable> failureExceptions;

    public static StepDescriptor convertFromStepExecution(StepExecution stepExecution) {
        StepDescriptor stepDescriptor = new StepDescriptor();
        stepDescriptor.commitCount = stepExecution.getCommitCount();
        Date endTime = stepExecution.getEndTime();
        if (endTime != null) {
            stepDescriptor.endTime = endTime;
        }
        Date startTime = stepExecution.getStartTime();
        if (startTime != null) {
            stepDescriptor.startTime = startTime;
        }
        stepDescriptor.exitStatus = stepExecution.getExitStatus();
        stepDescriptor.failureExceptions = stepExecution.getFailureExceptions();
        Date lastUpdated = stepExecution.getLastUpdated();
        if (lastUpdated != null) {
            stepDescriptor.lastUpdated = lastUpdated;
        }
        stepDescriptor.stepName = stepExecution.getStepName();
        stepDescriptor.status = stepExecution.getStatus();
        stepDescriptor.readCount = stepExecution.getReadCount();
        stepDescriptor.writeCount = stepExecution.getWriteCount();
        return stepDescriptor;
    }
}
