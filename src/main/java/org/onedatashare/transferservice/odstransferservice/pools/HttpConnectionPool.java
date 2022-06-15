package org.onedatashare.transferservice.odstransferservice.pools;

import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;

public class HttpConnectionPool implements ObjectPool<HttpClient> {
    AccountEndpointCredential credential;
    int bufferSize;
    public LinkedBlockingQueue<HttpClient> connectionPool;
    private boolean compress;


    public HttpConnectionPool(AccountEndpointCredential credential, int bufferSize) {
        this.credential = credential;
        this.bufferSize = bufferSize;
        this.connectionPool = new LinkedBlockingQueue<>();
    }

    @Override
    public void addObject() {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build();
        connectionPool.add(client);
    }

    @Override
    public void addObjects(int count) {
        for (int i = 0; i < count; i++) {
            this.addObject();
        }
    }

    @Override
    public HttpClient borrowObject() throws InterruptedException {
        return this.connectionPool.take();
    }

    @Override
    public void clear() {
        this.connectionPool.clear();
    }

    @Override
    public void close() {
        for (HttpClient httpClient : this.connectionPool) {
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
    public void invalidateObject(HttpClient httpClient) {
        this.connectionPool.remove(httpClient);
    }

    @Override
    public void returnObject(HttpClient httpClient) {
        this.connectionPool.add(httpClient);
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
}
