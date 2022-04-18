package org.onedatashare.transferservice.odstransferservice.service;

import org.apache.logging.log4j.util.Strings;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * @author deepika
 */
@Component
public class ChecksumAlgorithm {

    public String getAlgorithm(TransferJobRequest request) {
        if(checkSourceAndDestination(request, EndpointType.vfs, EndpointType.s3)){
            return "MD5";
        }
        if(request.getDestination().getType().equals(EndpointType.s3)){
            return "MD5";
        }
        if(checkSourceAndDestination(request, EndpointType.s3, EndpointType.vfs)){
            return "SHA-256";
        }
        if(request.getDestination().getType().equals(EndpointType.box)){
            return "SHA-1";
        }
        return "SHA-1";
    }

    private boolean checkSourceAndDestination(TransferJobRequest request, EndpointType source, EndpointType destination){
        return request.getDestination().getType().equals(source) && request.getSource().getType().equals(destination);
    }
}
