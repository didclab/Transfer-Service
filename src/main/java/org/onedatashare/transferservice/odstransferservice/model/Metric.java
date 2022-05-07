package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import org.springframework.batch.core.StepExecution;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;

@Data
public class Metric {
    double throughput;
    double memory;
    int cpus;
    LocalDate date;
    StepExecution stepExecution;
    DoubleSummaryStatistics throughputStat;
    List<Double> throughputList;
    int pipelining;

    public Metric(double throughput, StepExecution stepExecution) {
        this.cpus = Runtime.getRuntime().availableProcessors();
        this.memory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        date = LocalDate.now();
        this.throughput = throughput;
        throughputStat = new DoubleSummaryStatistics();
        throughputList = new ArrayList<>();
        pipelining = stepExecution.getCommitCount();
        this.stepExecution = stepExecution;
    }

    public void addBytesAndTime(long chunkStartTime, long bytesSent) {
        double chunkThroughput = (double) bytesSent / (System.nanoTime() - chunkStartTime);
        this.throughputList.add(chunkThroughput);
        this.throughputStat.accept(chunkThroughput);
    }

    public void addThroughput(double throughput) {
        this.throughputStat.accept(throughput);
        this.throughputList.add(throughput);
        this.date = LocalDate.now();
    }
}
