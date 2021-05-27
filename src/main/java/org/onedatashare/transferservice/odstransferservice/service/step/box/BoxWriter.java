package org.onedatashare.transferservice.odstransferservice.service.step.box;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

public class BoxWriter implements ItemWriter<DataChunk> {

    OAuthEndpointCredential credential;
    EntityInfo fileInfo;

    public BoxWriter(OAuthEndpointCredential oauthDestCredential, EntityInfo fileInfo) {
        this.credential = oauthDestCredential;
        fileInfo = fileInfo;
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {

    }
}
