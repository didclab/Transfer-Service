package org.onedatashare.transferservice.odstransferservice.pools;

import lombok.Getter;
import org.apache.commons.pool2.ObjectPool;
import org.apache.http.HttpClientConnection;
import org.apache.http.impl.nio.client.CloseableHttpPipeliningClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class ApacheHttpConnectionPool implements ObjectPool<CloseableHttpPipeliningClient> {

    AccountEndpointCredential credential;
    int bufferSize;
    int connectionCount;
    public LinkedBlockingQueue<CloseableHttpPipeliningClient> connectionPool;
    private boolean compress;
    public ApacheHttpConnectionPool(AccountEndpointCredential credential, int bufferSize){
        this.credential = credential;
        this.bufferSize = bufferSize;
        this.connectionPool = new LinkedBlockingQueue<>();
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    public boolean getCompress() {
        return this.compress;
    }

    public int getSize() {
        return this.connectionPool.size();
    }

    @Override
    public void addObject() throws Exception, IllegalStateException, UnsupportedOperationException {
        CloseableHttpPipeliningClient httpClient = HttpAsyncClients.createPipelining();
        connectionPool.add(httpClient);
    }

    @Override
    public void addObjects(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            this.addObject();
        }
    }

    @Override
    public CloseableHttpPipeliningClient borrowObject() throws Exception, NoSuchElementException, IllegalStateException {

        CloseableHttpPipeliningClient httpClient = this.connectionPool.take();
        httpClient.start();
        return httpClient;
    }

    @Override
    public void clear() throws Exception, UnsupportedOperationException {
        this.connectionPool.clear();
    }

    @Override
    public void close() {
        for (CloseableHttpPipeliningClient httpClient : this.connectionPool) {
            try {
                httpClient.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.connectionPool.remove(httpClient);
        }
    }

    @Override
    public int getNumActive() {
        return 0;
    }

    @Override
    public int getNumIdle() {
        return 0;
    }

    @Override
    public void invalidateObject(CloseableHttpPipeliningClient closeableHttpPipeliningClient) throws Exception {
        closeableHttpPipeliningClient.close();
        this.connectionPool.remove(closeableHttpPipeliningClient);
    }

    @Override
    public void returnObject(CloseableHttpPipeliningClient closeableHttpPipeliningClient) throws Exception {
        closeableHttpPipeliningClient.close();
        this.connectionPool.add(closeableHttpPipeliningClient);
    }
}
