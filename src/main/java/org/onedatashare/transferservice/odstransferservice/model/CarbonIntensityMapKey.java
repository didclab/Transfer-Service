package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CarbonIntensityMapKey {
    String ownerId;
    String transferNodeName;
    UUID jobUuid;
    LocalDateTime timeMeasuredAt;
}
