package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Value;

import java.io.Serializable;

@Value
public class TransferDetails implements Serializable {

    String fileName;
    Long duration;

}
