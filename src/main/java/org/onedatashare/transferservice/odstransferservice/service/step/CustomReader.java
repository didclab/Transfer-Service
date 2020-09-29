package org.onedatashare.transferservice.odstransferservice.service.step;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.file.*;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

public class CustomReader<T> extends AbstractItemCountingItemStreamItemReader<byte[]> implements ResourceAwareItemReaderItemStream<byte[]> {

    Logger logger = LoggerFactory.getLogger(CustomReader.class);

    public static final String DEFAULT_CHARSET = Charset.defaultCharset().name();

    private Resource resource;
    private SimpleBinaryBufferedReaderFactory simpleBinaryBufferedReaderFactory;
    private DefaultBufferedReaderFactory defaultBufferedReaderFactory;
    private BufferedReaderFactory bufferedReaderFactory;
    private InputStream reader;
    //No need now if we use our own ItemReader
    private String encoding;
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

    @Override
    protected byte[] doRead() {
        if (this.noInput) {
            return null;
        } else {
            byte[] bytes = this.readLine();
        }
        return new byte[0];
    }

    @Nullable
    private byte[] readLine() {
        if (this.reader == null) {
            throw new ReaderNotOpenException("Reader must be open before it can be read.");
        } else {
            byte[] bytes = null;
//            this.reader.
            try {
                return bytes;
            } catch (Exception var3) {
                this.noInput = true;
                logger.error("Unable to Read file");
            }
        }
        return null;
    }

    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(this.resource, "Input resource must be set");
        this.noInput = true;
        if (!this.resource.exists()) {
            logger.warn("Input resource does not exist " + this.resource.getDescription());
        } else if (!this.resource.isReadable()) {
            logger.warn("Input resource is not readable " + this.resource.getDescription());
        } else {
            this.reader = this.create(this.resource);
            this.noInput = false;
        }
    }

    private InputStream create(Resource res) throws IOException {
        URL u = new URL(res.getURL().toString());
        URLConnection conn = u.openConnection();
        return conn.getInputStream();
    }

    @Override
    protected void doClose() throws Exception {
        if (this.reader != null) {
            this.reader.close();
        }
    }
}
