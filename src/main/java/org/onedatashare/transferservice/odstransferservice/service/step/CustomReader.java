package org.onedatashare.transferservice.odstransferservice.service.step;

import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.onedatashare.transferservice.odstransferservice.model.StreamInput;
import org.onedatashare.transferservice.odstransferservice.model.StreamOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.file.BufferedReaderFactory;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.file.SimpleBinaryBufferedReaderFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

public class CustomReader<T> extends AbstractItemCountingItemStreamItemReader<byte[]> implements ResourceAwareItemReaderItemStream<byte[]>, InitializingBean {

    public static final String DEFAULT_CHARSET = Charset.defaultCharset().name();
    Logger logger = LoggerFactory.getLogger(CustomReader.class);
    int current = 0;
    private Resource resource;
    private SimpleBinaryBufferedReaderFactory simpleBinaryBufferedReaderFactory;
    private DefaultBufferedReaderFactory defaultBufferedReaderFactory;
    private final BufferedReaderFactory bufferedReaderFactory;
    private InputStream reader;
    private OutputStream outputStream;
    private long sizeBuffer;
    //No need now if we use our own ItemReader
    private final String encoding;
    private boolean noInput;


    public CustomReader() {
        this.encoding = DEFAULT_CHARSET;
        this.bufferedReaderFactory = new DefaultBufferedReaderFactory();
        this.setName(ClassUtils.getShortName(CustomReader.class));
        this.noInput = false;
    }


    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    /**
     * This will create the Input/Output Streams for the reader.
     * @return
     */
    @SneakyThrows
    @Override
    protected byte[] doRead() {
        byte[] data = new byte[4];
        int flag =this.reader.read(data);
        if(flag == -1){
            return null;
        }
        return data;
    }

    /**
     * This will create the FTPClient connection
     * @throws Exception
     */
    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(this.resource, "Input resource must be set");
        this.noInput = true;
        if (!this.resource.exists()) {
            //set as exceptions
            logger.warn("Input resource does not exist " + this.resource.getDescription());
        } else if (!this.resource.isReadable()) {
            //set as exceptions
            logger.warn("Input resource is not readable " + this.resource.getDescription());
        } else {
            logger.info(this.resource.getFilename());
            this.reader = StreamInput.createInputStream(this.resource.getFilename());
            StreamOutput.createOutputStream(this.resource.getFilename());
            this.noInput = false;
        }
    }

//    private InputStream create(Resource res) throws IOException {
//        URL u = new URL(res.getURL().toString());
//        URLConnection conn = u.openConnection();
//        return conn.getInputStream();
//    }

    @Override
    protected void doClose() throws Exception {
        if (this.reader != null) {
            this.reader.close();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(!this.noInput, "LineMapper is required");
    }
}
