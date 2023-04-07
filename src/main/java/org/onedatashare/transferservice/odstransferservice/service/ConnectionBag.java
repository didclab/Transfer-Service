package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.TransferOptions;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.*;
import org.onedatashare.transferservice.odstransferservice.pools.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * This class is responsible for preparing the SFTP, FTP, S3, Http connection Pool.
 */
@Getter
@Component
public class ConnectionBag {
    private Logger logger = LoggerFactory.getLogger(ConnectionBag.class);
    @Value("${ods.apache.httpclient.enabled}")
    boolean apacheHttpClientEnabled;
    private JschSessionPool sftpReaderPool;
    private JschSessionPool sftpWriterPool;
    private FtpConnectionPool ftpReaderPool;
    private FtpConnectionPool ftpWriterPool;
    private HttpConnectionPool httpReaderPool;
    private ApacheHttpConnectionPool apacheHttpReaderPool;
    private S3ConnectionPool s3ReaderPool;
    private S3ConnectionPool s3WriterPool;
    private GDriveConnectionPool googleDriveWriterPool;
    EndpointType readerType;
    EndpointType writerType;
    boolean readerMade;
    boolean writerMade;
    boolean compression;
    private TransferOptions transferOptions;

    public ConnectionBag() {
        readerMade = false;
        writerMade = false;
        compression = false;
    }

    public void preparePools(TransferJobRequest request) {
        this.transferOptions = request.getOptions();
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
        if (request.getSource().getType().equals(EndpointType.scp)) {
            readerMade = true;
            readerType = EndpointType.scp;
            this.createSftpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }
        if (request.getDestination().getType().equals(EndpointType.scp)) {
            writerMade = true;
            writerType = EndpointType.scp;
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
        if (request.getSource().getType().equals(EndpointType.http)) {
            readerMade = true;
            readerType = EndpointType.http;
            if(apacheHttpClientEnabled)
                this.createApacheHttpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
            else
                this.createHttpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }
        if (request.getDestination().getType().equals(EndpointType.gdrive)) {
            writerMade = true;
            writerType = EndpointType.gdrive;
            this.createGoogleDriveWriterPool(request.getDestination().getOauthDestCredential(), request.getOptions().getConcurrencyThreadCount(), request.getChunkSize());
        }
        if (request.getSource().getType().equals(EndpointType.s3)) {
            readerMade = true;
            readerType = EndpointType.s3;
            this.createS3ReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions());
        }
        if (request.getDestination().getType().equals(EndpointType.s3)) {
            writerMade = true;
            writerType = EndpointType.s3;
            this.createS3WriterPool(request.getDestination().getVfsDestCredential(), request.getOptions());
        }
    }

    public void closePools() {
        if (readerType != null) {
            switch (readerType) {
                case http:
                    if(!apacheHttpClientEnabled)
                        this.httpReaderPool.close();
                    else
                        this.apacheHttpReaderPool.close();
                    break;
                case ftp:
                    this.ftpReaderPool.close();
                    break;
                case sftp:
                    sftpReaderPool.close();
                    break;
                case s3:
                    s3ReaderPool.close();
                    break;
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
                case s3:
                    s3WriterPool.close();
                    break;
                case gdrive:
                    googleDriveWriterPool.close();
            }
        }
    }

    public void createFtpReaderPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {
        this.ftpReaderPool = new FtpConnectionPool(credential, chunkSize);
        try {
            this.ftpReaderPool.setCompression(this.transferOptions.getCompress());
            this.compression=this.transferOptions.getCompress();
            this.ftpReaderPool.addObjects(connectionCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createFtpWriterPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {
        this.ftpWriterPool = new FtpConnectionPool(credential, chunkSize);
        try {
            this.ftpWriterPool.setCompression(this.transferOptions.getCompress());
            this.compression = this.transferOptions.getCompress();
            this.ftpWriterPool.addObjects(connectionCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createSftpReaderPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {
        this.sftpReaderPool = new JschSessionPool(credential, chunkSize);
        this.sftpReaderPool.setCompression(this.transferOptions.getCompress());
        this.compression = this.transferOptions.getCompress();
        this.sftpReaderPool.addObjects(connectionCount);
    }

    public void createSftpWriterPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {
        this.sftpWriterPool = new JschSessionPool(credential, chunkSize);
        this.sftpWriterPool.setCompression(this.transferOptions.getCompress());
        this.compression = this.transferOptions.getCompress();
        this.sftpWriterPool.addObjects(connectionCount);
    }

    public void createHttpReaderPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {
        this.httpReaderPool = new HttpConnectionPool(credential, chunkSize);
        this.httpReaderPool.setCompress(false);
        this.httpReaderPool.addObjects(connectionCount);
        this.compression = false;
    }

    private void createGoogleDriveWriterPool(OAuthEndpointCredential oauthDestCredential, int concurrencyThreadCount, int chunkSize) {
        this.googleDriveWriterPool = new GDriveConnectionPool(oauthDestCredential, chunkSize);
        this.googleDriveWriterPool.setCompress(false);
        this.googleDriveWriterPool.addObjects(concurrencyThreadCount);
        this.compression = false;
    }

    public void createApacheHttpReaderPool(AccountEndpointCredential credential, int connectionCount, int chunkSize) {

        this.apacheHttpReaderPool = new ApacheHttpConnectionPool(credential, chunkSize);
        this.apacheHttpReaderPool.setCompress(false);
        try {
            this.apacheHttpReaderPool.addObjects(connectionCount);
        }catch(Exception ex){
            ex.printStackTrace();
        }
        this.compression = false;
    }

    public void createS3ReaderPool(AccountEndpointCredential credential, TransferOptions transferOptions) {
        this.compression = transferOptions.getCompress();
        this.s3ReaderPool = new S3ConnectionPool(credential, transferOptions);
        try {
            this.s3ReaderPool.addObjects(transferOptions.getConcurrencyThreadCount());
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public void createS3WriterPool(AccountEndpointCredential credential, TransferOptions transferOptions) {
        this.compression = transferOptions.getCompress();
        this.s3WriterPool = new S3ConnectionPool(credential, transferOptions);
        try {
            this.s3WriterPool.addObjects(transferOptions.getConcurrencyThreadCount());
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
