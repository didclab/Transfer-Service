package org.onedatashare.transferservice.odstransferservice.service.step.webdav;

import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import org.apache.http.HttpHeaders;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeWrite;
import org.springframework.batch.item.ItemWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class WebDAVWriter implements ItemWriter<DataChunk> {
    Logger logger = LoggerFactory.getLogger(WebDAVWriter.class);
    private EntityInfo fileInfo;
    private Sardine client;
    private AccountEndpointCredential credential;
    private String uri;
    private String fileName;

    public WebDAVWriter(AccountEndpointCredential credential, EntityInfo fileInfo, MetricsCollector metricsCollector, InfluxCache influxCache) {
        this.credential = credential;
        this.fileInfo = fileInfo;
        this.uri = credential.getUri();
    }

    @BeforeWrite
    public void beforeWrite(List<DataChunk> items){
        this.fileName = items.get(0).getFileName();
        if(client==null){
            if(this.credential.getUsername()!=null){
                logger.debug("Setting authentication credentials for WebDAV client");
                client= SardineFactory.begin(this.credential.getUsername(),this.credential.getSecret());
            }else{
                client= SardineFactory.begin();
            }
        }
    }


    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        for(DataChunk item : items){
            Map<String,String> headers = new HashMap<>();
            long bytesTo = item.getStartPosition()+item.getSize() - 1;
            headers.put(HttpHeaders.CONTENT_RANGE,"bytes "+item.getStartPosition()+"-"+bytesTo+"/"+this.fileInfo.getSize());
            boolean fileExist = client.exists(this.uri+item.getFileName());
            if(fileExist==true){
                logger.debug("File already exists. Overriding the file");
            }
            client.put(this.uri+item.getFileName(),new ByteArrayInputStream(item.getData()), URLConnection.guessContentTypeFromName(item.getFileName()), true);
        }
    }

    @AfterStep
    public void afterStep() throws IOException {
        client.shutdown();
    }
}
