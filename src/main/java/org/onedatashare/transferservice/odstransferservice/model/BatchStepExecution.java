package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.batch.core.StepExecution;

import java.time.LocalDateTime;
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

    private Long commitCount;

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
        LocalDateTime endTime = stepExecution.getEndTime();
        LocalDateTime lastUpdated = stepExecution.getLastUpdated();
        Integer version = stepExecution.getVersion();
        return new BatchStepExecutionBuilder()
                .readCount(stepExecution.getReadCount())
                .readSkipcount(stepExecution.getReadSkipCount())
                .writeSkipCount(stepExecution.getWriteSkipCount())
                .writeCount(stepExecution.getWriteCount())
                .commitCount(stepExecution.getCommitCount())
                .id(stepExecution.getId())
                .step_name(stepExecution.getStepName())
                .version(version.longValue())
                .jobInstanceId(stepExecution.getJobExecutionId())
                .startTime(stepExecution.getStartTime().toString())
                .endTime(endTime == null ? null : endTime.toString())
                .status(stepExecution.getStatus().toString())
                .exitCode(stepExecution.getExitStatus().getExitCode())
                .exitMessage(stepExecution.getExitStatus().getExitDescription())
                .lastUpdated(lastUpdated == null ? null : lastUpdated.toString())
                .filterCount(stepExecution.getFilterCount())
                .processSkipCount(stepExecution.getProcessSkipCount())
                .rollbackCount(stepExecution.getRollbackCount())
                .build();
    }
}
