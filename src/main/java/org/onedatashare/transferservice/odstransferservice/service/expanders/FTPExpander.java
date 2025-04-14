package org.onedatashare.transferservice.odstransferservice.service.expanders;

import lombok.SneakyThrows;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileType;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.DestinationChunkSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

@Service
public class FTPExpander extends DestinationChunkSize implements FileExpander {

    AccountEndpointCredential vfsCredential;
    List<EntityInfo> infoList;
    static final Logger logger = LoggerFactory.getLogger(FTPExpander.class);
    FileSystemOptions options;

    public FTPExpander(){
        this.options = generateOpts();
    }



    public static FileSystemOptions generateOpts() {
        FileSystemOptions opts = new FileSystemOptions();
        FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, true);
        FtpFileSystemConfigBuilder.getInstance().setFileType(opts, FtpFileType.BINARY);
        FtpFileSystemConfigBuilder.getInstance().setAutodetectUtf8(opts, true);
        FtpFileSystemConfigBuilder.getInstance().setControlEncoding(opts, "UTF-8");
        return opts;
    }

    @Override
    public void createClient(EndpointCredential credential) {
        this.vfsCredential = EndpointCredential.getAccountCredential(credential);
        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, this.vfsCredential.getUsername(), this.vfsCredential.getSecret());
        try {
            DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(options, auth);
        } catch (FileSystemException e) {
            e.printStackTrace();
        }
    }

    @SneakyThrows
    @Override
    public List<EntityInfo> expandedFileSystem(List<EntityInfo> userSelectedResources, String basePath) {
        this.infoList = userSelectedResources;
        List<EntityInfo> filesToTransferList = new LinkedList<>();
        Stack<FileObject> traversalStack = new Stack<>();
        FileSystemManager fsm = VFS.getManager();
        if(basePath.isEmpty() || basePath == null || !basePath.endsWith("/")) basePath += "/";
        if(infoList.isEmpty()){
            FileObject obj = fsm.resolveFile(this.vfsCredential.getUri() + basePath, this.options);
            traversalStack.push(obj);
        }else{
            for (EntityInfo e : this.infoList) {
                logger.info(this.vfsCredential.getUri() + basePath + e.getId());
                FileObject fObject = fsm.resolveFile(this.vfsCredential.getUri() + basePath + e.getId(), this.options);
                traversalStack.push(fObject);
            }
        }
        for (int files = Integer.MAX_VALUE; files > 0 && !traversalStack.isEmpty(); --files) {
            FileObject curr = traversalStack.pop();
            FileName fileName = curr.getName();
            URI uri = URI.create(fileName.getURI());
            logger.info(uri.toString());
            if (curr.getType() == FileType.FOLDER) {
                traversalStack.addAll(Arrays.asList(curr.getChildren()));
                //Add empty folders as well
                if (curr.getChildren().length == 0) {
                    EntityInfo fileInfo = new EntityInfo();
                    fileInfo.setId(fileName.getBaseName());
                    fileInfo.setPath(uri.getPath());
                    filesToTransferList.add(fileInfo);
                }
            } else if (curr.getType() == FileType.FILE) {

                //filePath = curr.getPublicURIString().substring(this.vfsCredential.getUri().length()+basePath.length());
                EntityInfo fileInfo = new EntityInfo();
                fileInfo.setId(curr.getName().getBaseName());
                fileInfo.setPath(uri.getPath());
                fileInfo.setSize(curr.getContent().getSize());
                filesToTransferList.add(fileInfo);
            }
        }
        return filesToTransferList;
    }
}
