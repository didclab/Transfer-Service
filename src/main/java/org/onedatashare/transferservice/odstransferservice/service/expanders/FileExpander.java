package org.onedatashare.transferservice.odstransferservice.service.expanders;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;

import java.util.List;

public interface FileExpander {

    public void createClient(EndpointCredential credential);

    public List<EntityInfo> expandedFileSystem(List<EntityInfo> userSelectedResources, String basePath);


}
