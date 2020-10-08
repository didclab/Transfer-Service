package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.StreamOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;


public class FTPWriter implements ItemWriter<DataChunk> {
    Logger logger = LoggerFactory.getLogger(FTPWriter.class);

    OutputStream writer;
//    @AfterStep
//    public void beforeStep() throws IOException {
//        writer.close();
//    }
    public void write(List<? extends DataChunk> list) throws Exception {
        for(DataChunk elem : list){
            logger.info(new String(elem.getData()));
        }
        writer = StreamOutput.getOutputStream();
        logger.info("Inside Writer----------------");
        for (DataChunk b : list) {
            writer.write(b.getData());
            writer.flush();
        }
    }
}
