package org.onedatashare.transferservice.odstransferservice.pools;

import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.springframework.batch.core.step.item.Chunk;

import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.concurrent.LinkedBlockingQueue;

public class HttpConnectionPool implements ObjectPool<HttpClient> {

    AccountEndpointCredential credential;
    int bufferSize;
    LinkedBlockingQueue<HttpClient> connectionPool;
    HttpClient borrowObj;

    public HttpConnectionPool(AccountEndpointCredential credential, int bufferSize){
        this.credential = credential;
        this.bufferSize = bufferSize;
        this.connectionPool = new LinkedBlockingQueue<>();
    }

    @Override
    public void addObject(){
        String[] hostAndPort = AccountEndpointCredential.uriFormat(credential, EndpointType.http);
        HttpClient client = HttpClient.newBuilder()
                    .authenticator(Authenticator.getDefault())
                    .proxy(ProxySelector.of(new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]))))
                    .version(HttpClient.Version.HTTP_2)
                    .build();
        connectionPool.add(client);
    }

    @Override
    public void addObjects(int count){
        for(int i = 0; i < count; i++) {
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
        for(HttpClient httpClient: this.connectionPool) {
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
    public void returnObject(HttpClient httpClient){
        this.connectionPool.add(httpClient);
    }
}
