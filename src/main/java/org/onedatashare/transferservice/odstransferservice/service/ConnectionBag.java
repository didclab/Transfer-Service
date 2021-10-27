package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.FtpConnectionPool;
import org.onedatashare.transferservice.odstransferservice.pools.JschSessionPool;
import org.springframework.stereotype.Component;

/**
 * This class is responsible for preparing the SFTP & FTP conneciton pool for readers and writers
 */
@Getter
@Component
public class ConnectionBag {
    private JschSessionPool sftpReaderPool;
    private JschSessionPool sftpWriterPool;
    private FtpConnectionPool ftpReaderPool;
    private FtpConnectionPool ftpWriterPool;

    EndpointType readerType;
    EndpointType writerType;
    boolean readerMade;
    boolean writerMade;
    boolean compression;

    public ConnectionBag() {
        readerMade = false;
        writerMade = false;
    }

    public void preparePools(TransferJobRequest request) {
        compression = request.getOptions().getCompress();
        if (request.getSource().getType().equals(EndpointType.sftp)) {
            readerMade = true;
            readerType = EndpointType.sftp;
            this.createSftpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }
        if (request.getDestination().getType().equals(EndpointType.sftp)) {
            writerMade = true;
            writerType = EndpointType.sftp;
            this.createSftpWriterPool(request.getDestination().getVfsDestCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }

        if (request.getSource().getType().equals(EndpointType.ftp)) {
            readerType = EndpointType.ftp;
            readerMade = true;
            this.createFtpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }
        if (request.getDestination().getType().equals(EndpointType.ftp)) {
            writerMade = true;
            writerType = EndpointType.ftp;
            this.createFtpWriterPool(request.getDestination().getVfsDestCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }
    }

    public void closePools() {
        if (readerType != null) {
            switch (readerType) {
                case ftp:
                    this.ftpReaderPool.close();
                    break;
                case sftp:
                    sftpReaderPool.close();
            }
        }
        if (writerType != null) {
            switch (writerType) {
                case ftp:
                    this.ftpWriterPool.close();
                    break;
                case sftp:
                    sftpWriterPool.close();
                    break;
            }
        }
    }

    public void createFtpReaderPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {
        this.ftpReaderPool = new FtpConnectionPool(credential, chunkSize);
        try {
            this.ftpReaderPool.addObjects(connectionCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createFtpWriterPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {
        this.ftpWriterPool = new FtpConnectionPool(credential, chunkSize);
        try {
            this.ftpWriterPool.addObjects(connectionCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createSftpReaderPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {
        this.sftpReaderPool = new JschSessionPool(credential, chunkSize);
        this.sftpReaderPool.setCompression(compression);
        this.sftpReaderPool.addObjects(connectionCount);
    }

    public void createSftpWriterPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {
        this.sftpWriterPool = new JschSessionPool(credential, chunkSize);
        this.sftpWriterPool.setCompression(compression);
        this.sftpWriterPool.addObjects(connectionCount);
    }
}
