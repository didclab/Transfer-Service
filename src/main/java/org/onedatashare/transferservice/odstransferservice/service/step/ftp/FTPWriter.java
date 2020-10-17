package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;


public class FTPWriter implements ItemWriter<DataChunk> {
    Logger logger = LoggerFactory.getLogger(FTPWriter.class);

    OutputStream writer = null;

    @AfterStep
    public void afterStep() throws IOException {
        writer.close();
    }

    public void write(List<? extends DataChunk> list) throws Exception {
        logger.info("Inside Writer---writing chunk of : "+list.get(0).getFileName());
        if(writer == null){
            writer = list.get(0).getOutputStream();
        }
        for (DataChunk b : list) {
            writer.write(b.getData());
            writer.flush();
        }
    }
}
