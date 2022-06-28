package org.onedatashare.transferservice.odstransferservice.model.credential;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.google.gson.annotations.JsonAdapter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import org.onedatashare.transferservice.odstransferservice.config.EpochMillisDateAdapter;

import java.util.Date;

/**
 * POJO for storing OAuth Credentials
 */
@Data
@AllArgsConstructor
public class OAuthEndpointCredential extends EndpointCredential {
    private String token;
    private boolean tokenExpires = false;
    @JsonAdapter(EpochMillisDateAdapter.class)
    private Date expiresAt;
    private String refreshToken;
    private boolean refreshTokenExpires = false;

    public OAuthEndpointCredential(String id){
        super(id);
    }
}

