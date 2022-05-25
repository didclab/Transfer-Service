package org.onedatashare.transferservice.odstransferservice.model.credential;

/**
 * Base class for storing one user credential
 */
public class EndpointCredential{
    protected String accountId;

    public EndpointCredential(){}
    public EndpointCredential(String accountId){
        this.accountId = accountId;
    }

    public AccountEndpointCredential getAccountCredential(EndpointCredential endpointCredential){
        if(endpointCredential instanceof  AccountEndpointCredential){
            return (AccountEndpointCredential) endpointCredential;
        }else{
            return null;
        }
    }
    public OAuthEndpointCredential getOAuthCredential(EndpointCredential endpointCredential){
        if(endpointCredential instanceof OAuthEndpointCredential){
            return (OAuthEndpointCredential) endpointCredential;
        }else{
            return null;
        }
    }
}