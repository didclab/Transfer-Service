package org.onedatashare.transferservice.odstransferservice.service.step.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxUser;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.AuthenticateCredentials;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.ByteArrayOutputStream;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.OWNER_ID;

public class BoxReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    private OAuthEndpointCredential credential;
    FilePartitioner filePartitioner;
    private BoxAPIConnection boxAPIConnection;
    private BoxFile currentFile;
    EntityInfo fileInfo;
    int retry;
    Logger logger = LoggerFactory.getLogger(BoxReader.class);

    String usersEmail;
    String ownerId;

    private AuthenticateCredentials authenticateCredentials;

    public BoxReader(OAuthEndpointCredential credential, EntityInfo fileInfo, AuthenticateCredentials authenticateCredentials) {
        this.credential = credential;
        this.setName(ClassUtils.getShortName(BoxReader.class));
        filePartitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.fileInfo = fileInfo;
        retry = 1;
        this.authenticateCredentials = authenticateCredentials;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution){
        this.ownerId = stepExecution.getJobParameters().getString(OWNER_ID);
    }
    /**
     * Read in those chunks
     *
     * @return
     */
    @Override
    protected DataChunk doRead() {
        if(this.credential.isTokenExpires()){
            this.credential = authenticateCredentials.checkExpiryAndGenerateNew( usersEmail, ODSConstants.BOX, ownerId);
            doOpen();
        }
        FilePart filePart = filePartitioner.nextPart();
        if (filePart == null) return null;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        this.currentFile.downloadRange(byteArray, filePart.getStart(), filePart.getEnd());
        DataChunk chunk = ODSUtility.makeChunk(filePart.getSize(), byteArray.toByteArray(), filePart.getStart(), Math.toIntExact(filePart.getPartIdx()), currentFile.getInfo().getName());
        logger.info(chunk.toString());
        return chunk;
    }

    /**
     * Open your connections, and get your streams
     *
     * @throws Exception
     */
    @Override
    protected void doOpen() {
        filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
        this.boxAPIConnection = new BoxAPIConnection(credential.getToken());
        BoxUser user = new BoxUser(this.boxAPIConnection, this.credential.getAccountId());
        BoxUser.Info userInfo = user.getInfo("login");
        this.usersEmail = userInfo.getLogin();
        this.currentFile = new BoxFile(this.boxAPIConnection, this.fileInfo.getId());
        this.currentFile.getInfo("name");
        this.boxAPIConnection.setMaxRetryAttempts(this.retry);
    }

    /**
     * Close your connection and destroy any clients used
     *
     * @throws Exception
     */
    @Override
    protected void doClose() {
        this.boxAPIConnection = null;
    }

    public void setMaxRetry(int attempt) {
        this.retry = attempt;
    }
}
