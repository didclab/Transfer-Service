package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class Processor implements ItemProcessor<DataChunk, DataChunk> {

    Logger logger = LoggerFactory.getLogger(Processor.class);

    @Override
    public DataChunk process(DataChunk dc) throws Exception {
//        System.out.println("Processor :"+dc.getData());
        return dc;
    }
}
