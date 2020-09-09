package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
//@StepScope
public class Processor implements ItemProcessor<byte[], DataChunk> {

    Logger logger = LoggerFactory.getLogger(Processor.class);

    private String basePath;
    private String filepath;

    @BeforeStep
    public void beforeStep(final StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        basePath = jobParameters.getString("destPath");
        filepath = "/tempOutput/firstTransferTest.txt";
    }



    @Override
    public DataChunk process(byte[] bytes) throws Exception {
        logger.info("Inside Processor");
        DataChunk dc = new DataChunk();
        logger.error("Inside Processor "+basePath);
        dc.setFileName(filepath);
        dc.setBasePath(basePath);
//        dc.openConnection();
        dc.setData(bytes);
        return dc;
    }
}
