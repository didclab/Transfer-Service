package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

import java.util.UUID;

@Data
public class StopJobRequest {
    UUID jobUuid;
    Integer jobId;
    String ownerId;
}
