package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.batch.core.StepExecution;

import java.sql.Timestamp;

@Data
@Builder
@AllArgsConstructor
public class BatchStepExecution {

    private Long id;

    private Long version;

    private String step_name;

    private Long jobInstanceId;

    private Timestamp startTime;

    private Timestamp endTime;

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

    private Timestamp lastUpdated;

    public static BatchStepExecution convertStepExecutionToMeta(StepExecution stepExecution) {
        return new BatchStepExecution.BatchStepExecutionBuilder()
                .readCount((long) stepExecution.getReadCount())
                .readSkipcount((long) stepExecution.getReadSkipCount())
                .writeSkipCount((long) stepExecution.getWriteSkipCount())
                .writeCount((long) stepExecution.getWriteCount())
                .commitCount(stepExecution.getCommitCount())
                .id(stepExecution.getId())
                .step_name(stepExecution.getStepName())

                .build();

    }
}
