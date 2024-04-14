package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FtpUtility {
    Logger logger = LoggerFactory.getLogger(FtpUtility.class);

    public static FileSystemOptions generateOpts() {
        FileSystemOptions opts = new FileSystemOptions();
        FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, true);
        FtpFileSystemConfigBuilder.getInstance().setFileType(opts, FtpFileType.BINARY);
        FtpFileSystemConfigBuilder.getInstance().setAutodetectUtf8(opts, true);
        FtpFileSystemConfigBuilder.getInstance().setControlEncoding(opts, "UTF-8");
        return opts;
    }
}
