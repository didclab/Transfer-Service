package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import com.onedatashare.commonservice.model.credential.AccountEndpointCredential;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SCP_MKDIR_CMD;


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
        if(credential.getSecret().contains("-----BEGIN RSA PRIVATE KEY-----")){
            try{
                return authenticateWithUserAndPrivateKey(credential, jsch, destCredUri, compression);
            } catch (JSchException e) {
                e.printStackTrace();
            }
        }else{
            try {
                return authenticateWithUserPass(credential, jsch, destCredUri, compression);
            } catch (JSchException e) {
                e.printStackTrace();
            }
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

    public static int checkAck(InputStream in, Logger logger) throws IOException {
        int b=in.read();
        // b may be 0 for success,
        //          1 for error,
        //          2 for fatal error,
        //          -1
        if(b==0) return b;
        if(b==-1) return b;

        if(b==1 || b==2){
            StringBuffer sb=new StringBuffer();
            int c;
            do {
                c=in.read();
                sb.append((char)c);
            }
            while(c!='\n');
            if(b==1){ // error
                logger.error(sb.toString());
            }
            if(b==2){ // fatal error
                logger.error(sb.toString());
            }
        }
        return b;
    }

    public static void okAck(OutputStream outputStream, byte[] socketBuffer) throws IOException {
        socketBuffer[0] = 0;
        outputStream.write(socketBuffer, 0, 1);
        outputStream.flush();
    }

    /**
     * Need to figure out what this is.
     * According to the link where I found a lot of this code it means: "read 0644"
     * @throws IOException
     */
    public static void someAck(InputStream inputStream, byte[] socketBuffer, Logger logger) throws IOException {
        int res = inputStream.read(socketBuffer, 0, 5);
        logger.info("Result from someAck: {}", res);
    }

    public static void readFileName(InputStream inputStream, byte[] socketBuffer, Logger logger) throws IOException {
        String file = null;
        for (int i = 0; ; i++) {
            inputStream.read(socketBuffer, i, 1);
            if (socketBuffer[i] == (byte)0x0a){
                file = new String(socketBuffer, 0, i);
                break;
            }
        }
        logger.info("File SCP is sending us: {} \n gotta ACK next", file);
    }

    public static long readFileSize(InputStream inputStream, byte[] socketBuffer, Logger logger) throws IOException {
        long filesize = 0L;

        while (true) {
            if (inputStream.read(socketBuffer, 0, 1) < 0) break;

            if (socketBuffer[0] == ' ') break;
            filesize = filesize * 10L + (long)(socketBuffer[0] - '0');
        }
        logger.info("The file size SCP gave us is: {}", filesize);
        return filesize;
    }

    public static void sendFileSize(OutputStream outputStream, String fileName, long fileSize) throws IOException {
        String command = "C0644 " + fileSize + " " + fileName + "\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
    }

    public static void mkdirSCP(Session session, String directoryPath, Logger logger) throws IOException, JSchException {
        String command = SCP_MKDIR_CMD + directoryPath;
        ChannelExec mkdirChannel = (ChannelExec) session.openChannel("exec");
        OutputStream mkdirOut = mkdirChannel.getOutputStream();
        InputStream mkdirIn = mkdirChannel.getInputStream();
        mkdirChannel.setCommand(command);
        mkdirChannel.connect();
        mkdirIn.read();
//        okAck(mkdirOut, new byte[1024]);
//        if(checkAck(mkdirIn, logger) != 0) throw new IOException("ACK failed for SCPReader mkdir -p : " + directoryPath);
        mkdirChannel.disconnect();
        mkdirIn.close();
        mkdirOut.close();
    }
}
