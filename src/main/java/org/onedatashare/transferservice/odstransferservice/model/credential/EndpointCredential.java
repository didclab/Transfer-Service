package org.onedatashare.transferservice.odstransferservice.model.credential;

import lombok.Getter;

/**
 * Base class for storing one user credential
 */

@Getter
public class EndpointCredential{
    protected String accountId;

    public EndpointCredential(){}
    public EndpointCredential(String accountId){
        this.accountId = accountId;
    }

}