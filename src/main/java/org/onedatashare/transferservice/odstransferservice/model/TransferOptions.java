package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TransferOptions {
    private Boolean compress=false;
    private Boolean encrypt=false;
    private String optimizer="";
    private boolean overwrite=false;
    private Integer retry=5;
    private Boolean verify=false;
    private int concurrencyThreadCount=1;
    private int parallelThreadCount=1;
    private int pipeSize=1;
}
