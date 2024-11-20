package org.onedatashare.transferservice.odstransferservice.service;

import com.onedatashare.commonutils.model.credential.AccountEndpointCredential;
import com.onedatashare.commonutils.model.credential.EndpointCredentialType;
import com.onedatashare.commonutils.model.credential.OAuthEndpointCredential;
import lombok.Getter;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.TransferOptions;
import org.onedatashare.transferservice.odstransferservice.pools.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * This class is responsible for preparing the SFTP, FTP, S3, Http connection Pool.
 */
@Getter
@Component
public class ConnectionBag {
    private Logger logger = LoggerFactory.getLogger(ConnectionBag.class);

    private JschSessionPool sftpReaderPool;
    private JschSessionPool sftpWriterPool;
    private FtpConnectionPool ftpReaderPool;
    private FtpConnectionPool ftpWriterPool;
    private HttpConnectionPool httpReaderPool;
    private S3ConnectionPool s3ReaderPool;
    private S3ConnectionPool s3WriterPool;
    private GDriveConnectionPool googleDriveWriterPool;
    EndpointCredentialType readerType;
    EndpointCredentialType writerType;
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
        if (request.getSource().getType().equals(EndpointCredentialType.sftp)) {
            readerMade = true;
            readerType = EndpointCredentialType.sftp;
            this.createSftpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount());
        }
        if (request.getDestination().getType().equals(EndpointCredentialType.sftp)) {
            writerMade = true;
            writerType = EndpointCredentialType.sftp;
            this.createSftpWriterPool(request.getDestination().getVfsDestCredential(), request.getOptions().getConcurrencyThreadCount());
        }
        if (request.getSource().getType().equals(EndpointCredentialType.scp)) {
            readerMade = true;
            readerType = EndpointCredentialType.scp;
            this.createSftpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount());
        }
        if (request.getDestination().getType().equals(EndpointCredentialType.scp)) {
            writerMade = true;
            writerType = EndpointCredentialType.scp;
            this.createSftpWriterPool(request.getDestination().getVfsDestCredential(), request.getOptions().getConcurrencyThreadCount());
        }
        if (request.getSource().getType().equals(EndpointCredentialType.ftp)) {
            readerType = EndpointCredentialType.ftp;
            readerMade = true;
            this.createFtpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount(), request.getConnectionBufferSize());
        }
        if (request.getDestination().getType().equals(EndpointCredentialType.ftp)) {
            writerMade = true;
            writerType = EndpointCredentialType.ftp;
            this.createFtpWriterPool(request.getDestination().getVfsDestCredential(), request.getOptions().getConcurrencyThreadCount(), request.getConnectionBufferSize());
        }
        if (request.getSource().getType().equals(EndpointCredentialType.http)) {
            readerMade = true;
            readerType = EndpointCredentialType.http;
            this.createHttpReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions().getConcurrencyThreadCount());
        }
        if (request.getDestination().getType().equals(EndpointCredentialType.gdrive)) {
            writerMade = true;
            writerType = EndpointCredentialType.gdrive;
            this.createGoogleDriveWriterPool(request.getDestination().getOauthDestCredential(), request.getOptions().getConcurrencyThreadCount());
        }
        if (request.getSource().getType().equals(EndpointCredentialType.s3)) {
            readerMade = true;
            readerType = EndpointCredentialType.s3;
            this.createS3ReaderPool(request.getSource().getVfsSourceCredential(), request.getOptions());
        }
        if (request.getDestination().getType().equals(EndpointCredentialType.s3)) {
            writerMade = true;
            writerType = EndpointCredentialType.s3;
            this.createS3WriterPool(request.getDestination().getVfsDestCredential(), request.getOptions());
        }
    }

    public void closePools() {
        if (readerType != null) {
            switch (readerType) {
                case http:
                    this.httpReaderPool.close();
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

    public void createSftpReaderPool(AccountEndpointCredential credential, int connectionCount) {
        this.sftpReaderPool = new JschSessionPool(credential);
        this.sftpReaderPool.setCompression(this.transferOptions.getCompress());
        this.compression = this.transferOptions.getCompress();
        this.sftpReaderPool.addObjects(connectionCount);
    }

    public void createSftpWriterPool(AccountEndpointCredential credential, int connectionCount) {
        this.sftpWriterPool = new JschSessionPool(credential);
        this.sftpWriterPool.setCompression(this.transferOptions.getCompress());
        this.compression = this.transferOptions.getCompress();
        this.sftpWriterPool.addObjects(connectionCount);
    }

    public void createHttpReaderPool(AccountEndpointCredential credential, int connectionCount) {
        this.httpReaderPool = new HttpConnectionPool(credential);
        this.httpReaderPool.setCompress(false);
        this.httpReaderPool.addObjects(connectionCount);
        this.compression = false;
    }

    private void createGoogleDriveWriterPool(OAuthEndpointCredential oauthDestCredential, int concurrencyThreadCount) {
        this.googleDriveWriterPool = new GDriveConnectionPool(oauthDestCredential);
        this.googleDriveWriterPool.setCompress(false);
        this.googleDriveWriterPool.addObjects(concurrencyThreadCount);
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
