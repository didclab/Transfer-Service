package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

public class Writer implements ItemWriter<TransferJobRequest> {

    @Override
    public void write(List<? extends TransferJobRequest> list) throws Exception {

    }
}
