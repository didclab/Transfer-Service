package org.onedatashare.transferservice.odstransferservice.service.DatabaseService;

import org.onedatashare.transferservice.odstransferservice.model.MetaDataDTO;

public interface MetaDataService {

    //Insert Statement
    MetaDataDTO saveOrUpdate(MetaDataDTO metaData);
    //ToDO
    //Query From the Job Table
    //To Calculate Total Time and Speed
    //Only can implement while transfer is successfully done
}
