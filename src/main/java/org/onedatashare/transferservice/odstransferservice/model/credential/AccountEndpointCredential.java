package org.onedatashare.transferservice.odstransferservice.model.credential;

import lombok.Data;

@Data
/**
 *
 */
public class AccountEndpointCredential extends EndpointCredential{
    private String uri; //the hostname and port to reach the server
    private String username; //this should be the username for the client
    private String secret; //This will contain the password of the resource you
    byte[] encryptedSecret;
}
