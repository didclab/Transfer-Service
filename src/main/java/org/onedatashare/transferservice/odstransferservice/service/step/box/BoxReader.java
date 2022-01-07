package org.onedatashare.transferservice.odstransferservice.service.step.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.ByteArrayOutputStream;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;

public class BoxReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    private OAuthEndpointCredential credential;
    int chunkSize;
    FilePartitioner filePartitioner;
    private BoxAPIConnection boxAPIConnection;
    private BoxFile currentFile;
    EntityInfo fileInfo;

    public BoxReader(OAuthEndpointCredential credential, int chunkSize, EntityInfo fileInfo){
        this.credential = credential;
        this.setName(ClassUtils.getShortName(BoxReader.class));
        this.chunkSize = Math.max(SIXTYFOUR_KB, chunkSize);
        filePartitioner = new FilePartitioner(this.chunkSize);
        this.fileInfo = fileInfo;
    }

    /**
     * This gets called before every single step executes and every step represents a single file fyi
     * @param stepExecution
     */
    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        filePartitioner.createParts(this.chunkSize, this.fileInfo.getId());
    }
    /**
     * Read in those chunks
     * @return
     */
    @Override
    protected DataChunk doRead() {
        FilePart filePart = filePartitioner.nextPart();
        if (filePart == null) return null;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        this.currentFile.downloadRange(byteArray, filePart.getStart(), filePart.getEnd());
        return ODSUtility.makeChunk(filePart.getSize(), byteArray.toByteArray(), filePart.getStart(), Math.toIntExact(filePart.getPartIdx()), currentFile.getInfo().getName());
    }

    /**
     * Open your connections, and get your streams
     * @throws Exception
     */
    @Override
    protected void doOpen() {
        this.boxAPIConnection = new BoxAPIConnection(credential.getToken());
        this.currentFile = new BoxFile(this.boxAPIConnection, this.fileInfo.getId());
    }

    /**
     * Close your connection and destroy any clients used
     * @throws Exception
     */
    @Override
    protected void doClose() {
        this.boxAPIConnection = null;
    }
}
