package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

@Data
public class TransferApplicationParams {
    int concurrency;
    int parallelism;
    int pipelining;
    long chunkSize;
    String transferNodeName;
}