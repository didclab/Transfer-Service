package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

@Service
public class VfsExpander {

    Logger logger;

    public VfsExpander() {
        this.logger = LoggerFactory.getLogger(FilePartitioner.class);
    }

    public List<EntityInfo> expandDirectory(List<EntityInfo> userResources, String basePath) {
        List<EntityInfo> endList = new ArrayList<>();
        Stack<File> traversalStack = new Stack<>(); //only directories on the stack.
        logger.info("Expanding files VFS: {}", userResources);
        if (userResources.size() == 0) return endList; //this case should never happen.
        for (EntityInfo fileInfo : userResources) {
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
                if(file.isDirectory()){
                    traversalStack.push(file);
                }else{
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
