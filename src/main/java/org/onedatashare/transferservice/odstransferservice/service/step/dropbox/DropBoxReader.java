package org.onedatashare.transferservice.odstransferservice.service.step.dropbox;

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DownloadBuilder;
import com.dropbox.core.v2.files.Metadata;
import com.onedatashare.commonservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.ByteArrayOutputStream;
import java.nio.file.Paths;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SOURCE_BASE_PATH;

public class DropBoxReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    Logger logger = LoggerFactory.getLogger(DropBoxReader.class);
    private final EntityInfo fileInfo;
    private final OAuthEndpointCredential credential;
    private String sBasePath;
    private FilePartitioner partitioner;

    private DbxClientV2 client;
    private DownloadBuilder requestSkeleton;
    private Metadata fileMetaData;


    public DropBoxReader(OAuthEndpointCredential credential, EntityInfo fileInfo) {
        this.credential = credential;
        this.fileInfo = fileInfo;
        this.partitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.setName(ClassUtils.getShortName(DropBoxReader.class));
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        if(sBasePath == null){
            this.sBasePath = Paths.get(sBasePath, fileInfo.getPath()).toString();
        }
        this.sBasePath = "";
        this.sBasePath = "/"+this.sBasePath;
        this.partitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @Override
    protected DataChunk doRead() throws Exception {
        FilePart currentPart = partitioner.nextPart();
        if (currentPart == null || currentPart.getStart() == currentPart.getEnd()) return null;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(currentPart.getSize());
        this.requestSkeleton.range(currentPart.getStart(), currentPart.getSize()).download(byteArrayOutputStream);
        DataChunk chunk = ODSUtility.makeChunk(currentPart.getSize(), byteArrayOutputStream.toByteArray(), currentPart.getStart(), Long.valueOf(currentPart.getPartIdx()).intValue(), this.fileMetaData.getName());
        byteArrayOutputStream.close();
        logger.info(chunk.toString());
        return chunk;
    }

    @Override
    protected void doOpen() throws DbxException {
        this.client = new DbxClientV2(ODSUtility.dbxRequestConfig, credential.getToken());
        this.requestSkeleton = this.client.files().downloadBuilder(this.fileInfo.getPath());
        this.fileMetaData = this.client.files().getMetadata(this.fileInfo.getPath());
    }

    @Override
    protected void doClose() {
        //this is not needed for some reason the client is auto destroyed somehow.
        //this could be through the closeable interface but not 100% sure will need to test/profile this
        this.client = null;
    }

}