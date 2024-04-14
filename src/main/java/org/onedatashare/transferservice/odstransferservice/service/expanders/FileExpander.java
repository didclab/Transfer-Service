package org.onedatashare.transferservice.odstransferservice.service.expanders;

import com.onedatashare.commonservice.model.credential.EndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;

import java.util.List;

public interface FileExpander {

    public void createClient(EndpointCredential credential);

    public List<EntityInfo> expandedFileSystem(List<EntityInfo> userSelectedResources, String basePath);


}
