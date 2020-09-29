package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class Processor implements ItemProcessor<byte[], byte[]> {

    Logger logger = LoggerFactory.getLogger(Processor.class);

    @Override
    public byte[] process(byte[] bytes) throws Exception {
        //System.out.println(bytes);
        return bytes;
    }
}
