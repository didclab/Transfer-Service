package org.onedatashare.transferservice.odstransferservice.service.DatabaseService;

import org.onedatashare.transferservice.odstransferservice.DataRepository.MetaDataRepository;
import org.onedatashare.transferservice.odstransferservice.model.MetaDataDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MetaDataServiceImplementation implements MetaDataService {
    @Autowired
    MetaDataRepository metaDataRepository;
    @Override
    public MetaDataDTO saveOrUpdate(MetaDataDTO metaData) {
        metaDataRepository.save(metaData);
        return metaData;
    }
    //ToDO
    //Write the Query Functions while Transfer is done
}
