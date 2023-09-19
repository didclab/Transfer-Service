package org.onedatashare.transferservice.odstransferservice.model;

import lombok.*;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;


import java.util.ArrayList;

@Data
@Getter
@NoArgsConstructor
public class TransferJobRequest {

    @NonNull private String ownerId;
    @NonNull private int connectionBufferSize;
    @NonNull private Source source;
    @NonNull private Destination destination;
    private TransferOptions options;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Destination {
        @NonNull private EndpointType type;
        String credId;
        private AccountEndpointCredential vfsDestCredential;
        private OAuthEndpointCredential oauthDestCredential;
        private String fileDestinationPath;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Source {
        @NonNull private EndpointType type;
        String credId;
        private AccountEndpointCredential vfsSourceCredential;
        private OAuthEndpointCredential oauthSourceCredential;
        private String fileSourcePath;
        @NonNull private ArrayList<EntityInfo> infoList;
    }
}