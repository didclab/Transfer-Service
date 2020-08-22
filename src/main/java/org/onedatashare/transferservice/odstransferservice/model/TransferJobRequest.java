package org.onedatashare.transferservice.odstransferservice.model;

import lombok.*;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;


import java.util.ArrayList;

@Data
@NoArgsConstructor
@Getter
public class TransferJobRequest {
    @NonNull private String ownerId;
    private int priority;
    @NonNull private String id;
    @NonNull private Source source;
    @NonNull private Destination destination;
    private TransferOptions options;


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Destination {
        @NonNull private EndpointType type;
        @NonNull private EndpointCredential credential;
        @NonNull private String credId;
        private EntityInfo info;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Source {
        @NonNull private EndpointType type;
        @NonNull private EndpointCredential credential;
        @NonNull private String credId;
        @NonNull private EntityInfo info;
        @NonNull private ArrayList<EntityInfo> infoList;
    }
}