package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.springframework.batch.item.ItemProcessor;

public class Processor implements ItemProcessor<byte[], byte[]> {

    @Override
    public byte[] process(byte[] str) throws Exception {
        System.out.println("Inside Processor");
//        System.out.println(str.toString());
        return str;
    }
}
