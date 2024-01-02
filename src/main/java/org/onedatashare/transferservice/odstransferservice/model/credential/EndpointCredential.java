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

}