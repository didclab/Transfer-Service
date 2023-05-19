package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.batch.core.*;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
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

    private String createTime;

    private String startTime;

    private String endTime;

    private BatchStatus status;

    private ExitStatus exitCode;

    private String exitMessage;

    private String lastUpdated;

    private Boolean isRunning;

    List<BatchStepExecution> batchSteps;

    Map<String, String> jobParameters;

    public static BatchJobData convertFromJobExecution(JobExecution jobExecution) {
        List<BatchStepExecution> steps = jobExecution.getStepExecutions().stream()
                .map(BatchStepExecution::convertStepExecutionToMeta)
                .collect(Collectors.toList());
        JobParameters jobParams = jobExecution.getJobParameters();
        BatchJobDataBuilder batchJobDataBuilder = new BatchJobDataBuilder();
        Map<String, JobParameter> map = jobParams.getParameters();
        Map<String, String> nextMap = new HashMap<>();
        for (String key : map.keySet()) {
            JobParameter jobParameter = map.get(key);
            if (jobParameter != null) {
                nextMap.put(key, jobParameter.toString());
            }
        }
        Date createTime = jobExecution.getCreateTime();
        Date startTime = jobExecution.getStartTime();
        Date endTime = jobExecution.getEndTime();
        Date lastUpdated = jobExecution.getLastUpdated();
        return batchJobDataBuilder
                .id(jobExecution.getId())
                .jobInstanceId(jobExecution.getJobInstance().getInstanceId())
                .version(Long.valueOf(jobExecution.getVersion()))
                .createTime(createTime == null ? null : createTime.toInstant().toString())
                .startTime(startTime == null ? null : startTime.toInstant().toString())
                .endTime(endTime == null ? null : endTime.toInstant().toString())
                .status(jobExecution.getStatus())
                .jobParameters(nextMap)
                .lastUpdated(lastUpdated == null ? null : lastUpdated.toInstant().toString())
                .exitCode(jobExecution.getExitStatus())
                .exitMessage(jobExecution.getExitStatus().getExitDescription())
                .isRunning(jobExecution.isRunning())
                .batchSteps(steps)
                .build();
    }
}

