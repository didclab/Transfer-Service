package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.springframework.batch.item.ItemProcessor;

public class Processor implements ItemProcessor<String, String> {

    @Override
    public String process(String str) throws Exception {
//        System.out.println("Inside Processor");
        return str;
    }
}
