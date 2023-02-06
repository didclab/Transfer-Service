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

}