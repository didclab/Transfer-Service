package org.onedatashare.transferservice.odstransferservice.pools;

import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class HttpConnectionPool implements ObjectPool<HttpClient> {
    AccountEndpointCredential credential;
    private boolean compress;
    BlockingQueue<HttpClient> connectionPool;

    public HttpConnectionPool(AccountEndpointCredential credential) {
        this.credential = credential;
        this.connectionPool = new LinkedBlockingQueue<>();
    }

    @Override
    public void addObject() {
        this.connectionPool.add(HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build());
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
        this.close();
    }

    @Override
    public void close() {
        for (HttpClient httpClient : this.connectionPool) {
            httpClient.close();
        }
        this.connectionPool.clear();
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
        httpClient.close();
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

}
