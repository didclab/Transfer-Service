package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TransferOptions {
    private Boolean compress;
    private Boolean encrypt;
    private String optimizer;
    private boolean overwrite;
    private Integer retry;
    private Boolean verify;
    private int concurrencyThreadCount;
    private int parallelThreadCount;
    private int pipeSize;
}
