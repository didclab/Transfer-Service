package org.onedatashare.transferservice.odstransferservice.service.step;


import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.TransferOptions;
import org.onedatashare.transferservice.odstransferservice.service.ConnectionBag;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3LargeFileWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Reader;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3SmallFileWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxReader;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxWriterLargeFile;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxWriterSmallFile;
import org.onedatashare.transferservice.odstransferservice.service.step.dropbox.DropBoxChunkedWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.dropbox.DropBoxReader;
import org.onedatashare.transferservice.odstransferservice.service.step.dropbox.DropBoxWriterSmallFile;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPReader;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.googleDrive.GDriveReader;
import org.onedatashare.transferservice.odstransferservice.service.step.googleDrive.GDriveResumableWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.googleDrive.GDriveSimpleWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.http.HttpReader;
import org.onedatashare.transferservice.odstransferservice.service.step.scp.SCPReader;
import org.onedatashare.transferservice.odstransferservice.service.step.scp.SCPWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SFTPReader;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SFTPWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.vfs.VfsReader;
import org.onedatashare.transferservice.odstransferservice.service.step.vfs.VfsWriter;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.FIVE_MB;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.TWENTY_MB;

@Service
public class ReaderWriterFactory {

    private final ConnectionBag connectionBag;
    private final InfluxCache influxCache;
    private final MetricsCollector metricsCollector;
    private final RetryTemplate retryTemplate;

    public ReaderWriterFactory(RetryTemplate retryTemplate, ConnectionBag connectionBag, InfluxCache influxCache, MetricsCollector metricsCollector) {
        this.connectionBag = connectionBag;
        this.influxCache = influxCache;
        this.metricsCollector = metricsCollector;
        this.retryTemplate = retryTemplate;
    }

    public void configureRetryTemplate(TransferOptions transferOptions) {
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(transferOptions.getRetry());
        this.retryTemplate.setRetryPolicy(simpleRetryPolicy);
    }

    public ItemReader<DataChunk> getRightReader(TransferJobRequest.Source source, EntityInfo fileInfo, TransferOptions transferOptions) {
        switch (source.getType()) {
            case http:
                HttpReader hr = new HttpReader(fileInfo, source.getVfsSourceCredential());
                hr.setPool(connectionBag.getHttpReaderPool());
                hr.setRetry(this.retryTemplate);
                return hr;
            case vfs:
                VfsReader vfsReader = new VfsReader(source.getVfsSourceCredential(), fileInfo);
                return vfsReader;
            case sftp:
                SFTPReader sftpReader = new SFTPReader(source.getVfsSourceCredential(), fileInfo, transferOptions.getPipeSize());
                sftpReader.setPool(connectionBag.getSftpReaderPool());
                return sftpReader;
            case ftp:
                FTPReader ftpReader = new FTPReader(source.getVfsSourceCredential(), fileInfo);
                ftpReader.setPool(connectionBag.getFtpReaderPool());
                return ftpReader;
            case s3:
                AmazonS3Reader amazonS3Reader = new AmazonS3Reader(source.getVfsSourceCredential(), fileInfo);
                amazonS3Reader.setPool(connectionBag.getS3ReaderPool());
                return amazonS3Reader;
            case box:
                BoxReader boxReader = new BoxReader(source.getOauthSourceCredential(), fileInfo);
                boxReader.setMaxRetry(transferOptions.getRetry());
                return boxReader;
            case dropbox:
                DropBoxReader dropBoxReader = new DropBoxReader(source.getOauthSourceCredential(), fileInfo);
                return dropBoxReader;
            case scp:
                SCPReader reader = new SCPReader(fileInfo);
                reader.setPool(connectionBag.getSftpReaderPool());
                return reader;
            case gdrive:
                GDriveReader dDriveReader = new GDriveReader(source.getOauthSourceCredential(), fileInfo);
                return dDriveReader;
        }
        return null;
    }

    public ItemWriter<DataChunk> getRightWriter(TransferJobRequest.Destination destination, EntityInfo fileInfo) {
        switch (destination.getType()) {
            case vfs:
                VfsWriter vfsWriter = new VfsWriter(destination.getVfsDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                return vfsWriter;
            case sftp:
                SFTPWriter sftpWriter = new SFTPWriter(destination.getVfsDestCredential(), this.metricsCollector, this.influxCache);
                sftpWriter.setPool(connectionBag.getSftpWriterPool());
                return sftpWriter;
            case ftp:
                FTPWriter ftpWriter = new FTPWriter(destination.getVfsDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                ftpWriter.setPool(connectionBag.getFtpWriterPool());
                return ftpWriter;
            case s3:
                if (fileInfo.getSize() < TWENTY_MB) {
                    AmazonS3SmallFileWriter amazonS3SmallFileWriter = new AmazonS3SmallFileWriter(destination.getVfsDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                    amazonS3SmallFileWriter.setPool(connectionBag.getS3WriterPool());
                    return amazonS3SmallFileWriter;
                } else {
                    AmazonS3LargeFileWriter amazonS3LargeFileWriter = new AmazonS3LargeFileWriter(destination.getVfsDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                    amazonS3LargeFileWriter.setPool(connectionBag.getS3WriterPool());
                    return amazonS3LargeFileWriter;
                }
            case box:
                if (fileInfo.getSize() < TWENTY_MB) {
                    BoxWriterSmallFile boxWriterSmallFile = new BoxWriterSmallFile(destination.getOauthDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                    return boxWriterSmallFile;
                } else {
                    BoxWriterLargeFile boxWriterLargeFile = new BoxWriterLargeFile(destination.getOauthDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                    return boxWriterLargeFile;
                }
            case dropbox:
                final long DROPBOX_SINGLE_UPLOAD_LIMIT = 150L * 1024L * 1024L;
                if (fileInfo.getSize() < DROPBOX_SINGLE_UPLOAD_LIMIT){
                    return new DropBoxWriterSmallFile(destination.getOauthDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                }else {
                    return new DropBoxChunkedWriter(destination.getOauthDestCredential(), this.metricsCollector, this.influxCache);
                }
            case scp:
                SCPWriter scpWriter = new SCPWriter(fileInfo, this.metricsCollector, this.influxCache);
                scpWriter.setPool(connectionBag.getSftpWriterPool());
                return scpWriter;
            case gdrive:
                if (fileInfo.getSize() < FIVE_MB) {
                    GDriveSimpleWriter writer = new GDriveSimpleWriter(destination.getOauthDestCredential(), fileInfo);
                    return writer;
                } else {
                    GDriveResumableWriter writer = new GDriveResumableWriter(destination.getOauthDestCredential(), fileInfo);
                    writer.setPool(connectionBag.getGoogleDriveWriterPool());
                    return writer;
                }
        }
        return null;
    }


}
