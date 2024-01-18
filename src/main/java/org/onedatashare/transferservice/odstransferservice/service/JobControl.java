package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.InfluxIOService;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3LargeFileWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Reader;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3SmallFileWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxReader;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxWriterLargeFile;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxWriterSmallFile;
import org.onedatashare.transferservice.odstransferservice.service.step.dropbox.DropBoxChunkedWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.dropbox.DropBoxReader;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.stream.Collectors;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.FIVE_MB;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.TWENTY_MB;


@Service
@NoArgsConstructor
@Getter
@Setter
public class JobControl {

    public TransferJobRequest request;

    Logger logger = LoggerFactory.getLogger(JobControl.class);

    @Autowired
    VfsExpander vfsExpander;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    ConnectionBag connectionBag;

    @Autowired
    JobCompletionListener jobCompletionListener;

    @Autowired
    MetricsCollector metricsCollector;

    @Autowired
    InfluxCache influxCache;

    @Autowired
    PlatformTransactionManager platformTransactionManager;

    @Autowired
    InfluxIOService influxIOService;

    @Autowired
    ThreadPoolManager threadPoolManager;

    private List<Flow> createConcurrentFlow(List<EntityInfo> infoList, String basePath) {
        if (this.request.getSource().getType().equals(EndpointType.vfs)) {
            infoList = vfsExpander.expandDirectory(infoList, basePath);
            logger.info("File list: {}", infoList);
        }
        return infoList.stream().map(file -> {
            String idForStep = "";
            if (!file.getId().isEmpty()) {
                idForStep = file.getId();
            } else {
                idForStep = file.getPath();
            }
            SimpleStepBuilder<DataChunk, DataChunk> stepBuilder = new StepBuilder(idForStep, this.jobRepository)
                    .chunk(this.request.getOptions().getPipeSize(), this.platformTransactionManager);
            stepBuilder
                    .reader(getRightReader(request.getSource().getType(), file))
                    .writer(getRightWriter(request.getDestination().getType(), file));
            if (this.request.getOptions().getParallelThreadCount() > 0) {
                stepBuilder.taskExecutor(threadPoolManager.parallelThreadPoolVirtual(request.getOptions().getParallelThreadCount() * request.getOptions().getConcurrencyThreadCount()));
            }
            stepBuilder.throttleLimit(64);
            return new FlowBuilder<Flow>(basePath + idForStep)
                    .start(stepBuilder.build()).build();
        }).collect(Collectors.toList());
    }

    protected ItemReader<DataChunk> getRightReader(EndpointType type, EntityInfo fileInfo) {
        switch (type) {
            case http:
                HttpReader hr = new HttpReader(fileInfo, request.getSource().getVfsSourceCredential());
                hr.setPool(connectionBag.getHttpReaderPool());
                return hr;
            case vfs:
                VfsReader vfsReader = new VfsReader(request.getSource().getVfsSourceCredential(), fileInfo);
                return vfsReader;
            case sftp:
                SFTPReader sftpReader = new SFTPReader(request.getSource().getVfsSourceCredential(), fileInfo, request.getOptions().getPipeSize());
                sftpReader.setPool(connectionBag.getSftpReaderPool());
                return sftpReader;
            case ftp:
                FTPReader ftpReader = new FTPReader(request.getSource().getVfsSourceCredential(), fileInfo);
                ftpReader.setPool(connectionBag.getFtpReaderPool());
                return ftpReader;
            case s3:
                AmazonS3Reader amazonS3Reader = new AmazonS3Reader(request.getSource().getVfsSourceCredential(), fileInfo);
                amazonS3Reader.setPool(connectionBag.getS3ReaderPool());
                return amazonS3Reader;
            case box:
                BoxReader boxReader = new BoxReader(request.getSource().getOauthSourceCredential(), fileInfo);
                boxReader.setMaxRetry(this.request.getOptions().getRetry());
                return boxReader;
            case dropbox:
                DropBoxReader dropBoxReader = new DropBoxReader(request.getSource().getOauthSourceCredential(), fileInfo);
                return dropBoxReader;
            case scp:
                SCPReader reader = new SCPReader(fileInfo);
                reader.setPool(connectionBag.getSftpReaderPool());
                return reader;
            case gdrive:
                GDriveReader dDriveReader = new GDriveReader(request.getSource().getOauthSourceCredential(), fileInfo);
                return dDriveReader;
        }
        return null;
    }

    protected ItemWriter<DataChunk> getRightWriter(EndpointType type, EntityInfo fileInfo) {
        switch (type) {
            case vfs:
                VfsWriter vfsWriter = new VfsWriter(request.getDestination().getVfsDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                return vfsWriter;
            case sftp:
                SFTPWriter sftpWriter = new SFTPWriter(request.getDestination().getVfsDestCredential(), this.metricsCollector, this.influxCache);
                sftpWriter.setPool(connectionBag.getSftpWriterPool());
                return sftpWriter;
            case ftp:
                FTPWriter ftpWriter = new FTPWriter(request.getDestination().getVfsDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                ftpWriter.setPool(connectionBag.getFtpWriterPool());
                return ftpWriter;
            case s3:
                if (fileInfo.getSize() < TWENTY_MB) {
                    AmazonS3SmallFileWriter amazonS3SmallFileWriter = new AmazonS3SmallFileWriter(request.getDestination().getVfsDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                    amazonS3SmallFileWriter.setPool(connectionBag.getS3WriterPool());
                    return amazonS3SmallFileWriter;
                } else {
                    AmazonS3LargeFileWriter amazonS3LargeFileWriter = new AmazonS3LargeFileWriter(request.getDestination().getVfsDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                    amazonS3LargeFileWriter.setPool(connectionBag.getS3WriterPool());
                    return amazonS3LargeFileWriter;
                }
            case box:
                if (fileInfo.getSize() < TWENTY_MB) {
                    BoxWriterSmallFile boxWriterSmallFile = new BoxWriterSmallFile(request.getDestination().getOauthDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                    return boxWriterSmallFile;
                } else {
                    BoxWriterLargeFile boxWriterLargeFile = new BoxWriterLargeFile(request.getDestination().getOauthDestCredential(), fileInfo, this.metricsCollector, this.influxCache);
                    return boxWriterLargeFile;
                }
            case dropbox:
                DropBoxChunkedWriter dropBoxChunkedWriter = new DropBoxChunkedWriter(request.getDestination().getOauthDestCredential(), this.metricsCollector, this.influxCache);
                return dropBoxChunkedWriter;
            case scp:
                SCPWriter scpWriter = new SCPWriter(fileInfo, this.metricsCollector, this.influxCache);
                scpWriter.setPool(connectionBag.getSftpWriterPool());
                return scpWriter;
            case gdrive:
                if (fileInfo.getSize() < FIVE_MB) {
                    GDriveSimpleWriter writer = new GDriveSimpleWriter(request.getDestination().getOauthDestCredential(), fileInfo);
                    return writer;
                } else {
                    GDriveResumableWriter writer = new GDriveResumableWriter(request.getDestination().getOauthDestCredential(), fileInfo);
                    writer.setPool(connectionBag.getGoogleDriveWriterPool());
                    return writer;
                }
        }
        return null;
    }

    public Job concurrentJobDefinition() {
        JobBuilder jobBuilder = new JobBuilder(this.request.getJobUuid().toString(), this.jobRepository);
        connectionBag.preparePools(this.request);
        List<Flow> flows = createConcurrentFlow(request.getSource().getInfoList(), request.getSource().getFileSourcePath());
        this.influxIOService.reconfigureBucketForNewJob(this.request.getOwnerId());
        Flow[] fl = new Flow[flows.size()];
        Flow f = new FlowBuilder<Flow>("splitFlow")
//                .split(this.threadPoolManager.stepTaskExecutorVirtual(this.request.getOptions().getConcurrencyThreadCount()))
                .split(this.threadPoolManager.stepTaskExecutorVirtual(this.request.getOptions().getConcurrencyThreadCount()))
                .add(flows.toArray(fl))
                .build();
        return jobBuilder
                .listener(jobCompletionListener)
                .start(f)
                .end()
                .build();
    }

}