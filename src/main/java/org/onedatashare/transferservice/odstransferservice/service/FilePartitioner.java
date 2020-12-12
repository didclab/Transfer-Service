package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
//import org.onedatashare.transferservice.odstransferservice.service.step.vfs.VfsReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;

public class FilePartitioner {
    Logger logger = LoggerFactory.getLogger(FilePartitioner.class);
    ConcurrentLinkedQueue<FilePart> queue;

    public FilePartitioner(){
        this.queue = new ConcurrentLinkedQueue<>();
    }

    public FilePart nextPart(){
        return this.queue.poll();
    }

    /**
     * Returning -1 means an error occured.
     * @param totalSize
     * @param fileName
     * @return
     */
    public int createParts(long totalSize, String fileName){
        if(totalSize < 1) return -1;
        if(totalSize <= SIXTYFOUR_KB){
            FilePart part = new FilePart();
            part.setFileName(fileName);
            part.setStart(0);
            part.setEnd(totalSize);
            part.setSize(totalSize);
            queue.add(part);
        }else{
            long startPosition = 0;
            long chunksOfSixtyFourKB = Math.floorDiv(totalSize, SIXTYFOUR_KB);
            for(int i = 0; i < chunksOfSixtyFourKB; i++){
                FilePart part = new FilePart();
                part.setFileName(fileName);
                part.setSize(SIXTYFOUR_KB);
                part.setStart(startPosition);
                startPosition+=SIXTYFOUR_KB;
                part.setEnd(startPosition);
                this.queue.add(part);
            }
            FilePart lastChunk = new FilePart();
            lastChunk.setStart(startPosition);
            lastChunk.setSize(totalSize-startPosition);
            lastChunk.setEnd(totalSize);
            queue.add(lastChunk);
        }
        logger.info("The total size of the queue after parsing file: " + fileName +" " +queue.size());
        return queue.size();
    }

}
