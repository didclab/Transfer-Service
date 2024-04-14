package org.onedatashare.transferservice.odstransferservice.pools;

import com.jcraft.jsch.ConfigRepository;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.onedatashare.commonservice.model.credential.AccountEndpointCredential;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SftpUtility;

import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

public class JschSessionPool implements ObjectPool<Session> {

    private final AccountEndpointCredential credential;
    public final LinkedBlockingQueue<Session> connectionPool;
    JSch jsch;
    private boolean compression;

    public JschSessionPool(AccountEndpointCredential credential){
        this.credential = credential;
        this.connectionPool = new LinkedBlockingQueue();
        jsch = new JSch();
        compression = false;
    }

    @Override
    public void addObject() {
        this.connectionPool.add(Objects.requireNonNull(SftpUtility.createJschSession(jsch, this.credential, compression)));
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
    public void invalidateObject(Session session) {
        session.disconnect();
        this.connectionPool.remove(session);
    }

    @Override
    public void returnObject(Session obj) {
        this.connectionPool.add(obj);
    }

    public void setCompression(boolean compression) {
        this.compression = compression;
    }

    public void invalidateAndCreateNewSession(Session session) {
        this.invalidateObject(session);
        this.addObject();
    }
}
