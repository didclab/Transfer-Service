package org.onedatashare.transferservice.odstransferservice.service.step.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.ClassUtils;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;

public class BoxWriter implements ItemWriter<DataChunk> {

    OAuthEndpointCredential credential;
    int chunkSize;
    FilePartitioner filePartitioner;
    private BoxAPIConnection boxAPIConnection;
//    private String sourcePath;
//    private BoxFile currentFile;
    Logger logger = LoggerFactory.getLogger(BoxWriter.class);
    EntityInfo fileInfo;
    private BoxFile.Info boxFileInfo;

    public BoxWriter(OAuthEndpointCredential oauthDestCredential, EntityInfo fileInfo) {
        this.credential = oauthDestCredential;
//        this.setName(ClassUtils.getShortName(BoxWriter.class));
        this.chunkSize = Math.max(SIXTYFOUR_KB, chunkSize);
        filePartitioner = new FilePartitioner(this.chunkSize);
        this.boxAPIConnection = new BoxAPIConnection(credential.getToken());
        this.fileInfo = fileInfo;
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        logger.info("You are here");
        for (int i = 0; i < items.size(); i++) {
            DataChunk chunk = items.get(i);
            InputStream inputStream = new ByteArrayInputStream(chunk.getData());
            File myFile = new File(this.fileInfo.getId());
//            FileInputStream stream = new FileInputStream(myFile);
            BoxFolder rootFolder = BoxFolder.getRootFolder(this.boxAPIConnection);
            boxFileInfo = rootFolder.uploadLargeFile(inputStream, myFile.getName(), myFile.length());
        }
    }
}
