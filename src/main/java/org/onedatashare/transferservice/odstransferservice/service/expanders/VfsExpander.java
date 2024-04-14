package org.onedatashare.transferservice.odstransferservice.service.expanders;

import com.onedatashare.commonservice.model.credential.EndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class VfsExpander implements FileExpander {

    Logger logger;

    public VfsExpander() {
        this.logger = LoggerFactory.getLogger(VfsExpander.class);
    }

    public void createClient(EndpointCredential credential) {}

    public List<EntityInfo> expandedFileSystem(List<EntityInfo> userSelectedResources, String basePath) {
        List<EntityInfo> endList = new ArrayList<>();
        Stack<File> traversalStack = new Stack<>(); //only directories on the stack.
        logger.info("Expanding files VFS: {}", userSelectedResources);
        if (userSelectedResources.isEmpty()) return endList; //this case should never happen.
        for (EntityInfo fileInfo : userSelectedResources) {
            Path path = Path.of(fileInfo.getPath());
            if (path.toFile().isDirectory()) {
                traversalStack.push(path.toFile());
            } else {
                endList.add(fileToEntity(path.toFile(), fileInfo.getChunkSize()));
            }
        }
        logger.info("Traversal Stack for expansion: {}", traversalStack);
        for (int i = Integer.MAX_VALUE; i > 0 && !traversalStack.isEmpty(); --i) {
            File resource = traversalStack.pop();
            File[] files = resource.listFiles();
            if (files == null) continue;
            for (File file : files) {
                if (file.isDirectory()) {
                    traversalStack.push(file);
                } else {
                    endList.add(fileToEntity(file, 0));
                }
            }
        }
        return endList;
    }

    private EntityInfo fileToEntity(File file, int chunkSize) {
        if (chunkSize < 64000) {
            chunkSize = 10000000; //10MB default
        }
        EntityInfo info = new EntityInfo();
        info.setSize(file.length());
        info.setPath(file.getAbsolutePath());
        info.setId(file.getName());
        info.setChunkSize(chunkSize);
        return info;
    }
}
