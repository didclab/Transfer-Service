package org.onedatashare.transferservice.odstransferservice.model.credential;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Date;

/**
 * POJO for storing OAuth Credentials
 */
@Data
@AllArgsConstructor
public class OAuthEndpointCredential extends EndpointCredential {
    private String token;
    private boolean tokenExpires = false;
    private Date expiresAt;
    private String refreshToken;
    private boolean refreshTokenExpires = false;

    public OAuthEndpointCredential(String id){
        super(id);
    }

}