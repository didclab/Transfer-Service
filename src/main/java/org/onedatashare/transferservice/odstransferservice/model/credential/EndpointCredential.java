package org.onedatashare.transferservice.odstransferservice.model.credential;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Base class for storing one user credential
 */
public class EndpointCredential{

    String accountId;
    public EndpointCredential(){}
    public EndpointCredential(String accountId){
        this.accountId = accountId;
    }

    public static AccountEndpointCredential getAccountCredential(EndpointCredential endpointCredential){
        if(endpointCredential instanceof  AccountEndpointCredential){
            return (AccountEndpointCredential) endpointCredential;
        }else{
            return null;
        }
    }
    public static OAuthEndpointCredential getOAuthCredential(EndpointCredential endpointCredential){
        if(endpointCredential instanceof OAuthEndpointCredential){
            return (OAuthEndpointCredential) endpointCredential;
        }else{
            return null;
        }
    }
}