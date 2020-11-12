package org.onedatashare.transferservice.odstransferservice.service.DatabaseService;

import org.onedatashare.transferservice.odstransferservice.model.MetaDataDTO;
import org.onedatashare.transferservice.odstransferservice.model.RsaCredential;

public interface RsaCredInterface {
    RsaCredential saveOrUpdate(RsaCredential metaData);
    String findById(String id) throws Exception;
}
