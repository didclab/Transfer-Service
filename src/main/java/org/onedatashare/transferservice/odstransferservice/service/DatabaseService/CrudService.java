package org.onedatashare.transferservice.odstransferservice.service.DatabaseService;

import org.onedatashare.transferservice.odstransferservice.model.MetaDataDTO;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CrudService {
    @Autowired
    MetaDataInterfaceImplementation metaDataServiceImplementation;
    public void insertIntoDatabase(TransferJobRequest transferJobRequest){
        MetaDataDTO metaDataDTO = MetaDataDTO.builder().id(transferJobRequest.getId())
                                    .source(transferJobRequest.getSource().getType().toString())
                                    .destination(transferJobRequest.getDestination().getType().toString())
                                    .type("mp4")
                                    .chunks(10).build();
        metaDataServiceImplementation.saveOrUpdate(metaDataDTO);
    }
}
