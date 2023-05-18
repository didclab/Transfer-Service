package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.batch.core.StepExecution;

import java.util.Date;

@Data
@Builder
@AllArgsConstructor
public class BatchStepExecution {

    private Long id;

    private Long version;

    private String step_name;

    private Long jobInstanceId;

    private String startTime;

    private String endTime;

    private String status;

    private Integer commitCount;

    private Long readCount;

    private Long filterCount;

    private Long writeCount;

    private Long readSkipcount;

    private Long writeSkipCount;

    private Long processSkipCount;

    private Long rollbackCount;

    private String exitCode;

    private String exitMessage;

    private String lastUpdated;

    public static BatchStepExecution convertStepExecutionToMeta(StepExecution stepExecution) {
        Date endTime = stepExecution.getEndTime();
        Date lastUpdated = stepExecution.getLastUpdated();
        Integer version = stepExecution.getVersion();
        return new BatchStepExecutionBuilder()
                .readCount((long) stepExecution.getReadCount())
                .readSkipcount((long) stepExecution.getReadSkipCount())
                .writeSkipCount((long) stepExecution.getWriteSkipCount())
                .writeCount((long) stepExecution.getWriteCount())
                .commitCount(stepExecution.getCommitCount())
                .id(stepExecution.getId())
                .step_name(stepExecution.getStepName())
                .version(version.longValue())
                .jobInstanceId(stepExecution.getJobExecutionId())
                .startTime(stepExecution.getStartTime().toInstant().toString())
                .endTime(endTime == null ? null : endTime.toInstant().toString())
                .status(stepExecution.getStatus().toString())
                .exitCode(stepExecution.getExitStatus().getExitCode())
                .exitMessage(stepExecution.getExitStatus().getExitDescription())
                .lastUpdated(lastUpdated == null ? null : lastUpdated.toInstant().toString())
                .filterCount((long) stepExecution.getFilterCount())
                .processSkipCount((long) stepExecution.getProcessSkipCount())
                .rollbackCount((long) stepExecution.getRollbackCount())
                .build();
    }
}
