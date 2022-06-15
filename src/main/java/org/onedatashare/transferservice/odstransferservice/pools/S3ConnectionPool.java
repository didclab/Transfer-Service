package org.onedatashare.transferservice.odstransferservice.pools;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.TransferOptions;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

public class S3ConnectionPool implements ObjectPool<AmazonS3> {

    private final TransferOptions transferOptions;
    public final LinkedBlockingQueue<AmazonS3> connectionPool;
    public final ClientConfiguration clientConfiguration;
    private final BasicAWSCredentials credentials;
    private final String region;
    Logger logger = LoggerFactory.getLogger(S3ConnectionPool.class);

    public S3ConnectionPool(AccountEndpointCredential cred, TransferOptions transferOptions) {
        this.connectionPool = new LinkedBlockingQueue<>();
        this.transferOptions = transferOptions;
        this.clientConfiguration = transferOptionsConvert();
        String[] temp = cred.getUri().split(":::");
        String bucketName = temp[1];
        this.region = temp[0];
        this.credentials = new BasicAWSCredentials(cred.getUsername(), cred.getSecret());
    }

    private ClientConfiguration transferOptionsConvert() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setUseTcpKeepAlive(true);
        if (transferOptions.getCompress()) {
            logger.info("S3 is using GZip to send the data");
            clientConfiguration.useGzip();
        }
        int[] bufferSendRecvSize = clientConfiguration.getSocketBufferSizeHints();
        logger.info("The TCP Send Buffer size= {} and Recv Buffer size= {} hints", bufferSendRecvSize[0], bufferSendRecvSize[1]);
        clientConfiguration.setSocketBufferSizeHints(bufferSendRecvSize[0], bufferSendRecvSize[1]);
        return clientConfiguration;
    }

    @Override
    public void addObject() {
        AmazonS3 client = AmazonS3ClientBuilder.standard()
                .withClientConfiguration(this.clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withChunkedEncodingDisabled(this.transferOptions.getVerify())
                .withRegion(region)
                .build();
        this.connectionPool.add(client);
    }

    @Override
    public void addObjects(int count) {
        for (int i = 0; i < count; i++) {
            this.addObject();
        }
    }

    @Override
    public AmazonS3 borrowObject() throws InterruptedException {
        return this.connectionPool.take();
    }

    @Override
    public void clear() {
        this.connectionPool.clear();
    }

    @Override
    public void close() {
        for (AmazonS3 client : this.connectionPool) {
            client.shutdown();
        }
        this.clear();
    }

    @Override
    public int getNumActive() {
        return this.connectionPool.size();
    }

    @Override
    public int getNumIdle() {
        return this.connectionPool.size();
    }

    @Override
    public void invalidateObject(AmazonS3 obj) {
        obj.shutdown();
        this.connectionPool.remove(obj);
    }

    @Override
    public void returnObject(AmazonS3 obj) {
        this.connectionPool.offer(obj);
    }

}
