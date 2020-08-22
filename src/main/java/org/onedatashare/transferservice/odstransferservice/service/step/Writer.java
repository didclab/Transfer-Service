package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.TransferService;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class Writer implements ItemWriter<String> {

    private TransferJobRequest transferJobRequest = TransferService.getTransferJobRequest();

    @Override
    public void write(List<? extends String> list) throws Exception {

    }
}
