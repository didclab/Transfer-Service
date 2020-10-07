package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.StreamOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FTPWriter implements ItemWriter<DataChunk> {
    Logger logger = LoggerFactory.getLogger(FTPWriter.class);
    public void write(List<? extends DataChunk> list) throws Exception {
        for(DataChunk elem : list){
            logger.info(new String(elem.getData()));
        }
        logger.info("Inside Writer----------------");
        for (DataChunk b : list) {
            StreamOutput.getOutputStream().write(b.getData());
            StreamOutput.getOutputStream().flush();
        }
    }
}
