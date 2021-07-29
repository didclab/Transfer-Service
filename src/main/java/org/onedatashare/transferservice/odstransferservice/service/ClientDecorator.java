package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;

public interface ClientManager {

    public void createClient(EndpointType type, EndpointCredential credential);
    public void deleteClient(EndpointType type, EndpointCredential credential);
    public void refreshClient();
    public void getClient(EndpointType type );
}
