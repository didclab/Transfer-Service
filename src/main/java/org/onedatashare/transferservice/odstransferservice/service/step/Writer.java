package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.StreamOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class Writer implements ItemWriter<byte[]> {
    Logger logger = LoggerFactory.getLogger(Writer.class);

    @Override
    public void write(List<? extends byte[]> list) throws Exception {
        logger.info("Inside Writer----------------");
        for (byte[] b : list) {
            StreamOutput.getOutputStream().write(b);
        }
    }
}
