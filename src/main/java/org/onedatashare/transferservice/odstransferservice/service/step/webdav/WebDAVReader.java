package org.onedatashare.transferservice.odstransferservice.service.step.webdav;

import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
public class WebDAVReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    Logger logger = LoggerFactory.getLogger(WebDAVReader.class);
    private final EntityInfo fileInfo;
    private final AccountEndpointCredential credential;
    private final FilePartitioner filePartitioner;
    private Sardine client;

    private String fileName;
    private String uri;

    public WebDAVReader(AccountEndpointCredential credential, EntityInfo fileInfo){
        this.fileInfo = fileInfo;
        this.filePartitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.setName(ClassUtils.getShortName(WebDAVReader.class));
        this.credential = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution){

        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
        this.fileName = fileInfo.getId();
        this.uri = credential.getUri() + Paths.get(fileInfo.getPath()).toString();
    }
    @Override
    protected DataChunk doRead() throws Exception {
        FilePart filePart = this.filePartitioner.nextPart();
        if (filePart == null) return null;
        Map<String,String> headers = new HashMap<>();
        headers.put(ODSConstants.RANGE,String.format(ODSConstants.byteRange,filePart.getStart(), filePart.getEnd()));
        InputStream response = client.get(this.uri,headers);
        byte[] body = response.readAllBytes();
        DataChunk chunk = ODSUtility.makeChunk(body.length, body, filePart.getStart(), Long.valueOf(filePart.getPartIdx()).intValue(), this.fileName);
        logger.info(chunk.toString());
        return chunk;
    }

    @Override
    protected void doOpen() throws Exception {
        if(this.credential!=null && this.credential.getUsername()!=null){
            client = SardineFactory.begin(this.credential.getUsername(),this.credential.getSecret());
        }else{
            client = SardineFactory.begin();
        }

    }

    @Override
    protected void doClose() throws Exception {
        client.shutdown();
    }
}
