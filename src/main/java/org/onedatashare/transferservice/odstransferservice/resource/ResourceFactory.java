package org.onedatashare.transferservice.odstransferservice.resource;

import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.service.abstracClass.Resource;
import org.springframework.stereotype.Component;

@Component
public class ResourceFactory {

    public Resource createResource(EndpointType type){
        switch (type){
            case box:
                return new BoxResource();
            case dropbox:
                return new DropBoxResource();
            case gdrive:
                return new GDriveResource();
            case sftp:
                return new VfsResource();
            case s3:
                return new S3Resource();
            case http:
                return new HttpResource();
            default:
                throw new UnsupportedOperationException("Unsupported EndpointType passed into createResource() in ResourceFactory");
        }
    }
}
