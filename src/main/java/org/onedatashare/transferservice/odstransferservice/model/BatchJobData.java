package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Builder
@Data
@AllArgsConstructor
public class BatchJobData {

    private Long id;

    private Long version;

    private Long jobInstanceId;

    private Timestamp createTime;

    private Timestamp startTime;

    private Timestamp endTime;

    private BatchStatus status;

    private ExitStatus exitCode;

    private String exitMessage;

    private Timestamp lastUpdated;

    private Boolean isRunning;

    List<BatchStepExecution> batchSteps;

    Map<String, String> jobParameters;

    public static BatchJobData convertFromJobExecution(JobExecution jobExecution) {
        List<BatchStepExecution> steps = jobExecution.getStepExecutions().stream()
                .map(BatchStepExecution::convertStepExecutionToMeta)
                .collect(Collectors.toList());
        Map<String, String> jobParams = jobExecution.getJobParameters()
                .getParameters()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

        return new BatchJobDataBuilder()
                .id(jobExecution.getId())
                .jobInstanceId(jobExecution.getJobInstance().getInstanceId())
                .version(Long.valueOf(jobExecution.getVersion()))
                .createTime(new Timestamp(jobExecution.getCreateTime().getTime()))
                .startTime(new Timestamp(jobExecution.getStartTime().getTime()))
                .endTime(new Timestamp(jobExecution.getEndTime().getTime()))
                .status(jobExecution.getStatus())
                .exitCode(jobExecution.getExitStatus())
                .isRunning(jobExecution.isRunning())
                .jobParameters(jobParams)
                .batchSteps(steps)
                .build();

    }
}

