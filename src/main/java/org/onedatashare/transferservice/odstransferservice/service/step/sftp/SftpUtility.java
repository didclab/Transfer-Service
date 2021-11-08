package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import lombok.SneakyThrows;
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

    public static Session createJschSession(JSch jsch, AccountEndpointCredential credential, boolean compression) {
        String noTypeUri = credential.getUri().replaceFirst("sftp://", "");
        String[] destCredUri = noTypeUri.split(":");
        boolean connected = false;
        Session jschSession = null;
        try {
            return authenticateWithUserAndPrivateKey(credential, jsch, destCredUri, compression);
        } catch (JSchException ignored) {
            ignored.printStackTrace();
        }
        try {
            return authenticateWithUserPass(credential, jsch, destCredUri, compression);
        } catch (JSchException ignored) {
            ignored.printStackTrace();
        }
        return null;
    }

    public static Session authenticateWithUserAndPrivateKey(AccountEndpointCredential credential, JSch jsch, String[] destCredUri, boolean compression) throws JSchException {
        jsch.addIdentity("randomName", credential.getSecret().getBytes(), null, null);
        Session jschSession = jsch.getSession(credential.getUsername(), destCredUri[0], Integer.parseInt(destCredUri[1]));
        jschSession.setConfig("StrictHostKeyChecking", "no");
        if(compression){
            jschSession.setConfig("compression.s2c", "zlib@openssh.com,zlib,none");
            jschSession.setConfig("compression.c2s", "zlib@openssh.com,zlib,none");
            jschSession.setConfig("compression_level", "5");
        }
        jschSession.connect();
        return jschSession;
    }

    public static Session authenticateWithUserPass(AccountEndpointCredential credential, JSch jsch, String[] destCredUri, boolean compression) throws JSchException {
        Session jschSession = jsch.getSession(credential.getUsername(), destCredUri[0], Integer.parseInt(destCredUri[1]));
        jschSession.setConfig("StrictHostKeyChecking", "no");
        jschSession.setPassword(credential.getSecret());
        if(compression){
            jschSession.setConfig("compression.s2c", "zlib@openssh.com,zlib,none");
            jschSession.setConfig("compression.c2s", "zlib@openssh.com,zlib,none");
            jschSession.setConfig("compression_level", "5");
        }
        jschSession.connect();
        return jschSession;
    }

    @SneakyThrows
    public static ChannelSftp createRemoteFolder(ChannelSftp channelSftp, String remotePath) {
        String[] folders = remotePath.split("/");
        if(remotePath.startsWith("/")){
            folders[0] = "/" + folders[0];
        }
        for (String folder : folders) {
            if (!folder.isEmpty()) {
                boolean flag = true;
                try {
                    channelSftp.cd(folder);
                } catch (SftpException e) {
                    flag = false;
                }
                if (!flag) {
                    try {
                        channelSftp.mkdir(folder);
                        channelSftp.cd(folder);
                        flag = true;
                    } catch (SftpException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return channelSftp;
    }

    public static boolean mkdir(ChannelSftp channelSftp, String basePath){
        try {
            channelSftp.mkdir(basePath);
            return true;
        } catch (SftpException sftpException) {
            sftpException.printStackTrace();
        }
        return false;
    }
}
