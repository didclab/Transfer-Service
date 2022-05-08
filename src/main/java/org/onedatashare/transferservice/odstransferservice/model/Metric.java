package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import org.springframework.batch.core.StepExecution;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.PIPELINING;

@Data
public class Metric {
    double throughput;
    double memory;
    int cpus;
    LocalDate date;
    StepExecution stepExecution;
    DoubleSummaryStatistics throughputStat;
    int pipelining;

    public Metric(double throughput, StepExecution stepExecution, int pipeSize) {
        this.cpus = Runtime.getRuntime().availableProcessors();
        this.memory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        date = LocalDate.now();
        this.throughput = throughput;
        throughputStat = new DoubleSummaryStatistics();
        pipelining = pipeSize;
        this.stepExecution = stepExecution;
    }

}
