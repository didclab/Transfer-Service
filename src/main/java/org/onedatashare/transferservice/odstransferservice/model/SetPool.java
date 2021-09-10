package org.onedatashare.transferservice.odstransferservice.model;

import org.apache.commons.pool2.ObjectPool;

public interface SetPool {

    public void setPool(ObjectPool<DataChunk> connectionPool);
}
