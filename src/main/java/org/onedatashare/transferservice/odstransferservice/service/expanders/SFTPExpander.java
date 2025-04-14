package org.onedatashare.transferservice.odstransferservice.service.expanders;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.DestinationChunkSize;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class SFTPExpander extends DestinationChunkSize implements FileExpander {

    AccountEndpointCredential credential;
    ChannelSftp channelSftp;
    List<EntityInfo> infoList;

    @SneakyThrows
    @Override
    public void createClient(EndpointCredential cred) {
        this.credential = EndpointCredential.getAccountCredential(cred);
        Session jschSession = null;
        boolean connected = false;
        JSch jsch = new JSch();
        String[] typeAndUri = credential.getUri().split("://"); //split out the sftp partof the string
        String host = "";
        String port = "22";
        if(typeAndUri[1].contains(":")){
            String[] hostAndPort = typeAndUri[1].split(":");
            host = hostAndPort[0];
            port = hostAndPort[1];
        }
        try {
            jsch.addIdentity("randomName", credential.getSecret().getBytes(), null, null);
            jschSession = jsch.getSession(credential.getUsername(), host, Integer.parseInt(port));
            jschSession.setConfig("StrictHostKeyChecking", "no");
            jschSession.connect();
            connected = true;
        } catch (JSchException ignored) {
            connected = false;
        }
        if (!connected) {
            try {
                jschSession = jsch.getSession(credential.getUsername(), host, Integer.parseInt(port));
                jschSession.setConfig("StrictHostKeyChecking", "no");
                jschSession.setPassword(credential.getSecret());
                jschSession.connect();
                connected = true;
            } catch (JSchException ignored) {
                connected = false;
            }
        }
        assert jschSession != null;
//        if (!jschSession.isConnected()) {
//            throw new JSchException("Unable to authenticate with the password/pem file");
//        }
        ChannelSftp channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
        channelSftp.connect();
        this.channelSftp = channelSftp;
    }

    @SneakyThrows
    @Override
    public List<EntityInfo> expandedFileSystem(List<EntityInfo> userSelectedResources, String basePath) {
        //if(!basePath.endsWith("/")) basePath +="/";
        this.infoList = userSelectedResources;
        List<EntityInfo> filesToTransferList = new LinkedList<>();
        Stack<ChannelSftp.LsEntry> traversalStack = new Stack<>();
        HashMap<ChannelSftp.LsEntry, String> entryToFullPath = new HashMap<>();
        if (basePath.isEmpty()) basePath = channelSftp.pwd();
        if(!basePath.endsWith("/")) basePath += "/";
        if (userSelectedResources.isEmpty()) {
            Vector<ChannelSftp.LsEntry> fileVector = channelSftp.ls(basePath);
            for (ChannelSftp.LsEntry curr : fileVector) {
                entryToFullPath.put(curr, basePath + curr.getFilename());
                traversalStack.add(curr);
            }
        } else {
            for (EntityInfo e : userSelectedResources) {
                String path = basePath + e.getPath();
                Vector<ChannelSftp.LsEntry> fileVector = channelSftp.ls(path);
                if(fileVector.size() == 1) {
                    ChannelSftp.LsEntry curr = fileVector.get(0);
                    entryToFullPath.put(curr, path);
                    traversalStack.add(fileVector.get(0));
                }else{
                    for (ChannelSftp.LsEntry curr : fileVector) {
                        entryToFullPath.put(curr, path + curr.getFilename());
                        traversalStack.add(curr);
                    }
                }
            }
        }

        while (!traversalStack.isEmpty()) {
            ChannelSftp.LsEntry curr = traversalStack.pop();
            String fullPath = entryToFullPath.remove(curr);
            if (curr.getFilename().equals(".") || curr.getFilename().equals("..")) { //skip these two
                continue;
            }
            if (curr.getAttrs().isDir()) {
                Vector<ChannelSftp.LsEntry> children = channelSftp.ls(fullPath);
                if (children.size() == 0) {//this should include the empty directory
                    EntityInfo fileInfo = new EntityInfo();
                    fileInfo.setId(curr.getFilename());
                    fileInfo.setSize(curr.getAttrs().getSize());
                    fileInfo.setPath(fullPath);
                } else {
                    for (ChannelSftp.LsEntry f : children) {
                        entryToFullPath.put(f, fullPath + "/" + f.getFilename());
                        traversalStack.add(f);
                    }
                }
            } else if (!curr.getAttrs().isDir()) {
                EntityInfo fileInfo = new EntityInfo();
                fileInfo.setPath(fullPath);
                fileInfo.setId(curr.getFilename());
                fileInfo.setSize(curr.getAttrs().getSize());
                filesToTransferList.add(fileInfo);
            }
        }
        return filesToTransferList;
    }
}
