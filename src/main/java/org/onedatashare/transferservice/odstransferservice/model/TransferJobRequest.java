package org.onedatashare.transferservice.odstransferservice.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.onedatashare.commonutils.model.credential.AccountEndpointCredential;
import com.onedatashare.commonutils.model.credential.OAuthEndpointCredential;
import lombok.*;
import com.onedatashare.commonutils.model.credential.EndpointCredentialType;

import java.util.List;
import java.util.UUID;

@Data
@Getter
@NoArgsConstructor
public class TransferJobRequest {

    @NonNull
    private String ownerId;
    private int connectionBufferSize;
    @NonNull
    private Source source;
    @NonNull
    private Destination destination;
    private TransferOptions options;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private UUID jobUuid;
    public String transferNodeName;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Destination {
        @NonNull
        private EndpointCredentialType type;
        String credId;
        private AccountEndpointCredential vfsDestCredential;
        private OAuthEndpointCredential oauthDestCredential;
        private String fileDestinationPath;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Source {
        @NonNull
        private EndpointCredentialType type;
        String credId;
        private AccountEndpointCredential vfsSourceCredential;
        private OAuthEndpointCredential oauthSourceCredential;
        private String fileSourcePath;
        @NonNull
        private List<EntityInfo> infoList;
    }
}