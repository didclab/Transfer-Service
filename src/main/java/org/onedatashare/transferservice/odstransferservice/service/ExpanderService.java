package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.expanders.*;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ExpanderService {

    public TransferJobRequest expandFiles(TransferJobRequest job){
        TransferJobRequest.Source source = job.getSource();
        switch(source.getType()){
        case vfs -> {
            VfsExpander vfsExpander = new VfsExpander();
            List<EntityInfo> fileInfo = vfsExpander.expandedFileSystem(source.getInfoList(), source.getFileSourcePath());
            job.getSource().setInfoList(fileInfo);
        }

        case http -> {
            HttpExpander httpExpander = new HttpExpander();
            List<EntityInfo> fileInfo = httpExpander.expandedFileSystem(source.getInfoList(), source.getFileSourcePath());
            job.getSource().setInfoList(fileInfo);
        }

        case box -> {
            BoxExpander boxExpander = new BoxExpander();
            List<EntityInfo> fileInfo = boxExpander.expandedFileSystem(source.getInfoList(), source.getFileSourcePath());
            job.getSource().setInfoList(fileInfo);
        }
        case dropbox -> {
            DropBoxExpander dropBoxExpander = new DropBoxExpander();
            List<EntityInfo> fileInfo = dropBoxExpander.expandedFileSystem(source.getInfoList(), source.getFileSourcePath());
            job.getSource().setInfoList(fileInfo);
        }
        case gdrive -> {
            GDriveExpander gDriveExpander = new GDriveExpander();
            List<EntityInfo> fileInfo = gDriveExpander.expandedFileSystem(source.getInfoList(), source.getFileSourcePath());
            job.getSource().setInfoList(fileInfo);
        }
        case s3 -> {
            S3Expander s3Expander = new S3Expander();
            List<EntityInfo> fileInfo = s3Expander.expandedFileSystem(source.getInfoList(), source.getFileSourcePath());
            job.getSource().setInfoList(fileInfo);

        }
        case sftp -> {
            SFTPExpander sftpExpander = new SFTPExpander();
            List<EntityInfo> fileInfo = sftpExpander.expandedFileSystem(source.getInfoList(), source.getFileSourcePath());
            job.getSource().setInfoList(fileInfo);
        }
        case ftp -> {
            FTPExpander ftpExpander = new FTPExpander();
            List<EntityInfo> fileInfo = ftpExpander.expandedFileSystem(source.getInfoList(), source.getFileSourcePath());
            job.getSource().setInfoList(fileInfo);
        }
    }
                return job;
}
}
