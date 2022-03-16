package org.onedatashare.transferservice.odstransferservice.service.DatabaseService;

import org.onedatashare.transferservice.odstransferservice.model.MetaDataDTO;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CrudService {
    @Autowired
    MetaDataInterfaceImplementation metaDataServiceImplementation;
    public void insertBeforeTransfer(TransferJobRequest transferJobRequest){
        MetaDataDTO metaDataDTO = MetaDataDTO.builder().id(transferJobRequest.getJobId())
                .source(transferJobRequest.getSource().getType().toString())
                .destination(transferJobRequest.getDestination().getType().toString())
                .parallelism(transferJobRequest.getOptions().getParallelThreadCount())
                .concurrency(transferJobRequest.getOptions().getConcurrencyThreadCount())
                .compress(transferJobRequest.getOptions().getCompress().toString())
                .pipelining(transferJobRequest.getOptions().getPipeSize())
                .retry(transferJobRequest.getOptions().getRetry())
                .chunks(10).build();
        metaDataServiceImplementation.saveOrUpdate(metaDataDTO);
    }

    public void insertAfterTransfer(MetaDataDTO metaDataDTO){
        metaDataServiceImplementation.saveOrUpdate(metaDataDTO);
    }


}
