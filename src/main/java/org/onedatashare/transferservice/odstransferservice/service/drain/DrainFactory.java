package org.onedatashare.transferservice.odstransferservice.service.drain;

import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.service.abstracClass.Drain;

public class DrainFactory {
    public Drain createDrain(EndpointType type){
        switch (type){
            default:
                throw new UnsupportedOperationException("Unsupported EndpointType passed into createResource() in ResourceFactory");
        }
    }
}
