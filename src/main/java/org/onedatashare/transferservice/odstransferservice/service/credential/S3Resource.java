package org.onedatashare.transferservice.odstransferservice.service.credential;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.service.abstracClass.Drain;
import org.onedatashare.transferservice.odstransferservice.service.abstracClass.Resource;
import org.onedatashare.transferservice.odstransferservice.service.abstracClass.Tap;

public class S3Resource extends Resource {
    @Override
    public Tap getTap(EntityInfo baseInfo, EntityInfo relativeInfo) throws Exception {
        return null;
    }

    @Override
    public Drain getDrain(EntityInfo baseInfo, EntityInfo relativeInfo) throws Exception {
        return null;
    }
}
