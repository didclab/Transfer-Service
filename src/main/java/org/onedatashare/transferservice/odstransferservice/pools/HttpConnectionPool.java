package org.onedatashare.transferservice.odstransferservice.pools;

import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.Executors;

public class HttpConnectionPool implements ObjectPool<HttpClient> {
    AccountEndpointCredential credential;
    private boolean compress;
    HttpClient client;

    public HttpConnectionPool(AccountEndpointCredential credential) {
        this.credential = credential;
    }

    @Override
    public void addObject() {
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .executor(Executors.newCachedThreadPool())
                .connectTimeout(Duration.ofSeconds(20))
                .build();
    }

    @Override
    public void addObjects(int count) {
        for (int i = 0; i < count; i++) {
            this.addObject();
        }
    }

    @Override
    public HttpClient borrowObject() throws InterruptedException {
//        return this.connectionPool.take();
        return this.client;
    }

    @Override
    public void clear() {
        this.client = null;
    }

    @Override
    public void close() {
//        for (HttpClient httpClient : this.connectionPool) {
//            this.connectionPool.remove(httpClient);
//        }
        this.client = null;
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
//        this.connectionPool.remove(httpClient);
        this.client = null;
    }

    @Override
    public void returnObject(HttpClient httpClient) {
//        this.connectionPool.add(httpClient);
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    public boolean getCompress() {
        return this.compress;
    }

    public int getSize() {
        return 1;
    }
}
