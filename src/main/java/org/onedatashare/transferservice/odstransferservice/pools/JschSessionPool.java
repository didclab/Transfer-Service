package org.onedatashare.transferservice.odstransferservice.pools;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SftpUtility;

import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

public class JschSessionPool implements ObjectPool<Session> {

    private final AccountEndpointCredential credential;
    private final int bufferSize;
    private final LinkedBlockingQueue<Session> connectionPool;
    JSch jsch;

    public JschSessionPool(AccountEndpointCredential credential, int bufferSize){
        this.credential = credential;
        this.bufferSize = bufferSize;
        this.connectionPool = new LinkedBlockingQueue();
        jsch = new JSch();
    }

    @Override
    public void addObject() {
        this.connectionPool.add(Objects.requireNonNull(SftpUtility.createJschSession(jsch, this.credential)));
    }

    @Override
    public void addObjects(int count) {
        for(int i = 0; i < count; i++){
            this.addObject();
        }
    }

    @Override
    public Session borrowObject() throws InterruptedException {
        return this.connectionPool.take();
    }

    @Override
    public void clear() {
        this.connectionPool.removeIf(channelSftp -> !channelSftp.isConnected());
    }

    @Override
    public void close() {
        while(getNumActive() > 0){
            for(Session session : this.connectionPool){
                session.disconnect();
                this.connectionPool.remove(session);
            }
        }
    }

    @Override
    public int getNumActive() {
        int count = 0;
        for(Session session : this.connectionPool){
            if(session.isConnected()){
                count ++;
            }
        }
        return count;
    }

    @Override
    public int getNumIdle() {
        int count = 0;
        for(Session session : this.connectionPool){
            if(!session.isConnected()){
                count ++;
            }
        }
        return count;
    }

    @Override
    public void invalidateObject(Session obj) {
        for(Session session : this.connectionPool){
            if(session.equals(obj)){
                session.disconnect();
                this.connectionPool.remove(session);
            }
        }
    }

    @Override
    public void returnObject(Session obj) {
        this.connectionPool.add(obj);
    }
}