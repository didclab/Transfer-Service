package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SftpUtility {

    Logger logger = LoggerFactory.getLogger(SftpUtility.class);

    public static ChannelSftp openSFTPConnection(JSch jsch, AccountEndpointCredential cred) throws JSchException {
        jsch.addIdentity("randomName", cred.getSecret().getBytes(), null, null);
        String[] destCredUri = cred.getUri().split(":");
        Session jschSession = jsch.getSession(cred.getUsername(), destCredUri[0], Integer.parseInt(destCredUri[1]));
        jschSession.setConfig("StrictHostKeyChecking", "no");
        jschSession.connect();
        Channel sftp = jschSession.openChannel("sftp");
        ChannelSftp channelSftp = (ChannelSftp) sftp;
        channelSftp.connect();
        return channelSftp;
    }

    public static ChannelSftp createConnection(JSch jsch, AccountEndpointCredential credential) throws JSchException {
        Session jschSession = null;
        String noTypeUri = credential.getUri().replaceFirst("sftp://", "");
        String[] destCredUri = noTypeUri.split(":");
        boolean connected = false;
        try {
            jsch.addIdentity("randomName", credential.getSecret().getBytes(), null, null);
            jschSession = jsch.getSession(credential.getUsername(), destCredUri[0], Integer.parseInt(destCredUri[1]));
            jschSession.connect();
            jschSession.setConfig("StrictHostKeyChecking", "no");
            connected = true;
        } catch (JSchException ignored) {
            connected = false;
        }
        if(!connected){
            try {
                jschSession = jsch.getSession(credential.getUsername(), destCredUri[0], Integer.parseInt(destCredUri[1]));
                jschSession.setConfig("StrictHostKeyChecking", "no");
                jschSession.setPassword(credential.getSecret());
                jschSession.connect();
                connected = true;
            } catch (JSchException ignored) {
                connected = false;
            }
        }
        if(!connected){
            return null;
        }
        ChannelSftp channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
        channelSftp.connect();
        return channelSftp;
    }
}
