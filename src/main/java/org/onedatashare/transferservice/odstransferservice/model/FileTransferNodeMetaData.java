package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.UUID;

@Data
@AllArgsConstructor
@Builder
public class FileTransferNodeMetaData implements Serializable {

    //ods metrics
    String odsOwner;
    String nodeName;
    UUID nodeUuid;
    Boolean runningJob;
    Boolean online;
    long jobId;
    UUID jobUuid;
}
