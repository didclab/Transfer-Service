package org.onedatashare.transferservice.odstransferservice.service.expanders;

import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ExpanderFactory {

    public List<EntityInfo> getExpander(TransferJobRequest.Source source){
        switch (source.getType()){
            case vfs -> {
                VfsExpander vfsExpander = new VfsExpander();
                vfsExpander.createClient(source.getVfsSourceCredential());
                return vfsExpander.expandedFileSystem(source.getInfoList(), source.getFileSourcePath());
            }
            case http -> {
                HttpExpander httpExpander = new HttpExpander();
                httpExpander.createClient(source.getVfsSourceCredential());
                return httpExpander.expandedFileSystem(source.getInfoList(), source.getFileSourcePath());
            }
        }
        return source.getInfoList();
    }
}
