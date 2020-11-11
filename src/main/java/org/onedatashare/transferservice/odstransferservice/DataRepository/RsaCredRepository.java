package org.onedatashare.transferservice.odstransferservice.DataRepository;

import org.onedatashare.transferservice.odstransferservice.model.MetaDataDTO;
import org.onedatashare.transferservice.odstransferservice.model.RsaCredential;
import org.springframework.data.repository.CrudRepository;

public interface RsaCredRepository extends CrudRepository<RsaCredential,Long> {
}
