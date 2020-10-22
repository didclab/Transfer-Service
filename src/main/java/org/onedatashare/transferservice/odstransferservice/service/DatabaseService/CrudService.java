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
        MetaDataDTO metaDataDTO = MetaDataDTO.builder().id(transferJobRequest.getId())
                                    .source(transferJobRequest.getSource().getType().toString())
                                    .destination(transferJobRequest.getDestination().getType().toString())
                                    .chunks(10).build();
        metaDataServiceImplementation.saveOrUpdate(metaDataDTO);
    }

    public void insertAfterTransfer(MetaDataDTO metaDataDTO){
        metaDataServiceImplementation.saveOrUpdate(metaDataDTO);
    }


}
