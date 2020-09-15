package org.onedatashare.transferservice.odstransferservice.DataRepository;

import org.onedatashare.transferservice.odstransferservice.model.MetaDataDTO;
import org.springframework.data.repository.CrudRepository;

public interface MetaDataRepository extends CrudRepository<MetaDataDTO,Long> {
}
