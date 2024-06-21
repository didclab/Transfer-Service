package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

@Data
public class CarbonMeasureRequest {
    public String transferNodeName;
    public String sourceIp;
    public String destinationIp;
}
