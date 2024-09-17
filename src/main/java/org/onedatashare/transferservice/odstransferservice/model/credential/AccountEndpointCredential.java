package org.onedatashare.transferservice.odstransferservice.model.credential;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.lang.StringUtils;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountEndpointCredential extends EndpointCredential{
    private String uri; //the hostname and port to reach the server
    private String username; //this should be the username for the client
    @ToString.Exclude
    private String secret; //This will contain the password of the resource you
    @JsonIgnore
    byte[] encryptedSecret;

    public static String[] uriFormat(AccountEndpointCredential credential, EndpointType type) {
        String noTypeUri = "";
        if(type.equals(EndpointType.sftp)){
            noTypeUri = credential.getUri().replaceFirst("sftp://", "");
        }else if(type.equals(EndpointType.ftp)){
            noTypeUri = credential.getUri().startsWith("ftps://") ?
                    credential.getUri().replaceFirst("ftps://", "")
                   : credential.getUri().replaceFirst("ftp://", "");
        }
        else{
            noTypeUri = credential.getUri().replaceFirst("http://", "");
        }
        return noTypeUri.split(":");
    }
}
