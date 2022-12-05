package org.onedatashare.transferservice.odstransferservice.service.step;

import lombok.Getter;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.service.MetricCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterRead;
import org.springframework.batch.core.annotation.AfterWrite;
import org.springframework.batch.core.annotation.BeforeRead;
import org.springframework.batch.core.annotation.BeforeWrite;

import java.time.LocalDateTime;
import java.util.List;

public class ODSBaseWriter {

    protected StepExecution stepExecution;

    Logger logger = LoggerFactory.getLogger(ODSBaseWriter.class);

    @Setter
    MetricsCollector metricsCollector;
    private LocalDateTime writeStartTime;
    private LocalDateTime readStartTime;

    @Getter
    @Setter
    private MetricCache metricCache;

    @BeforeWrite
    public void beforeWrite() {
        this.writeStartTime = LocalDateTime.now();
        logger.info("Before write start time {}", this.writeStartTime);
    }

    @AfterWrite
    public void afterWrite(List<? extends DataChunk> items) {
        ODSConstants.metricsForOptimizerAndInflux(items, this.writeStartTime, logger, stepExecution, metricCache, metricsCollector);
    }

    @BeforeRead
    public void beforeRead() {
        this.readStartTime = LocalDateTime.now();
        logger.info("Before read start time {}", this.readStartTime);
    }

    @AfterRead
    public void afterRead(DataChunk item) {
        ODSConstants.metricsForOptimizerAndInflux(item, this.readStartTime, logger, stepExecution, metricCache, metricsCollector);
    }
}
