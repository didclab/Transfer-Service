package org.onedatashare.transferservice.odstransferservice.model.credential;

import lombok.Data;
import lombok.ToString;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;

@Data
public class AccountEndpointCredential extends EndpointCredential{
    private String uri; //the hostname and port to reach the server
    private String username; //this should be the username for the client
    @ToString.Exclude
    private String secret; //This will contain the password of the resource you
    byte[] encryptedSecret;

    public static String[] uriFormat(AccountEndpointCredential credential, EndpointType type) {
        String noTypeUri = "";
        if(type.equals(EndpointType.sftp)){
            noTypeUri = credential.getUri().replaceFirst("sftp://", "");
        }else if(type.equals(EndpointType.ftp)){
            noTypeUri = credential.getUri().replaceFirst("ftp://", "");
        }
        return noTypeUri.split(":");
    }
}
