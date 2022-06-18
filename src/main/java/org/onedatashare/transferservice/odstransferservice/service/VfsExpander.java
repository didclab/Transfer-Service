package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

@Service
public class VfsExpander {

    public List<EntityInfo> expandDirectory(List<EntityInfo> userResources, String basePath) {
        List<EntityInfo> endList = new ArrayList<>();
        Stack<File> traversalStack = new Stack<>(); //only directories on the stack.
        if (userResources.size() == 0) return endList; //this case should never happen.
        for (EntityInfo fileInfo : userResources) {
            Path path = Path.of(fileInfo.getPath());
            if (path.toFile().isDirectory()) {
                traversalStack.push(path.toFile());
            } else if (path.toFile().isFile()) {
                endList.add(fileToEntity(path.toFile(), fileInfo.getChunkSize()));
            }
        }
        for (int i = Integer.MAX_VALUE; i > 0 && !traversalStack.isEmpty(); --i) {
            File resource = traversalStack.pop();
            File[] files = resource.listFiles();
            if (files == null) continue;
            for (File file : files) {
                if (file.isFile()) {
                    endList.add(fileToEntity(file, 0));
                } else if (file.isDirectory()) {
                    traversalStack.push(file);
                }
            }
        }
        return endList;
    }

    public EntityInfo fileToEntity(File file, int chunkSize) {
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
