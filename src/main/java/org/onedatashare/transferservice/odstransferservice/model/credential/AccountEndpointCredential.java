package org.onedatashare.transferservice.odstransferservice.model.credential;

import lombok.Data;
import lombok.ToString;

@Data
public class AccountEndpointCredential extends EndpointCredential{
    private String uri; //the hostname and port to reach the server
    private String username; //this should be the username for the client
    @ToString.Exclude
    private String secret; //This will contain the password of the resource you
    byte[] encryptedSecret;
}
