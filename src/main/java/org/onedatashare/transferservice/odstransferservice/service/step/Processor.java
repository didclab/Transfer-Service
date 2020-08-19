package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.springframework.batch.item.ItemProcessor;

public class Processor implements ItemProcessor<TransferJobRequest, String> {

    @Override
    public String process(TransferJobRequest transferJobRequest) throws Exception {

        return null;
    }
}
