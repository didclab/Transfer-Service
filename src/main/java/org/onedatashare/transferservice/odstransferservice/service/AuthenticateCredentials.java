package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;


@Service
public class AuthenticateCredentials {
    RestTemplate credentialTemplate; 
    String baseUrl = "http://EndPointCredentialService/{}/{}/{}";

    public AuthenticateCredentials(RestTemplate credentialTemplate){
        this.credentialTemplate = credentialTemplate;
    }
        public OAuthEndpointCredential checkExpiryAndGenerateNew(String userId, String type, String accountId){
            String endpoint = baseUrl.replaceFirst("\\{}", userId)
                                     .replaceFirst("\\{}", type)
                                     .replaceFirst("\\{}", accountId);
            return this.credentialTemplate.getForObject(endpoint, OAuthEndpointCredential.class);
        }
}
