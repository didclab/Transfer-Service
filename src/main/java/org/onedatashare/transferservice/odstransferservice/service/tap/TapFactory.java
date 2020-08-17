package org.onedatashare.transferservice.odstransferservice.service.tap;

import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.service.abstracClass.Tap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TapFactory {

    public Tap createTap(EndpointType type){
    switch (type){
        default:
            throw new UnsupportedOperationException("Unsupported EndpointType passed into createResource() in ResourceFactory");
    }

    }
}
