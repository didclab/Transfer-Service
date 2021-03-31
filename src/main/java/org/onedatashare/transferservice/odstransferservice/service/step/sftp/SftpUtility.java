package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SftpUtility {

    Logger logger = LoggerFactory.getLogger(SftpUtility.class);

    public static ChannelSftp openSFTPConnection(JSch jsch, AccountEndpointCredential cred) throws JSchException {
        ChannelSftp channelSftp = null;
        Session jschSession = null;
//            jsch.addIdentity("/home/vishal/.ssh/ods-bastion-dev.pem");
//            jsch.setKnownHosts("/home/vishal/.ssh/known_hosts");
        jsch.addIdentity("randomName", cred.getSecret().getBytes(), null, null);
        String[] destCredUri = cred.getUri().split(":");
        jschSession = jsch.getSession(cred.getUsername(), destCredUri[0], Integer.parseInt(destCredUri[1]));
        jschSession.setConfig("StrictHostKeyChecking", "no");
        jschSession.connect();
        Channel sftp = jschSession.openChannel("sftp");
        channelSftp = (ChannelSftp) sftp;
        channelSftp.connect();
        return channelSftp;
    }
}
