package org.onedatashare.transferservice.odstransferservice.model.metrics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CarbonScore {
    public int avgCarbon;

    public CarbonScore(){
        this.avgCarbon = 0;
    }
}
