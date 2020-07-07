package org.onedatashare.transferservice.odstransferservice.model.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.onedatashare.transferservice.odstransferservice.model.core.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.core.EntityInfo;


import java.util.ArrayList;

@Data
@NoArgsConstructor
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
        @NonNull private String credId;
        private EntityInfo info;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Source {
        @NonNull private EndpointType type;
        @NonNull private String credId;
        @NonNull private EntityInfo info;
        @NonNull private ArrayList<EntityInfo> infoList;
    }
}