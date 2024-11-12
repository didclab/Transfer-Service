package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

@Data
public class CarbonIpEntry {
    String ip;
    int carbonIntensity;
    double lat;
    double lon;
}
