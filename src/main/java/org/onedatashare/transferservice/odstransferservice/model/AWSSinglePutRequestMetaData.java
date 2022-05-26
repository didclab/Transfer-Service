package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@Getter
@Setter
public class AWSSinglePutRequestMetaData {
    private Queue<DataChunk> dataChunkPriorityQueue;
    Logger logger = LoggerFactory.getLogger(AWSSinglePutRequestMetaData.class);

    public AWSSinglePutRequestMetaData(){
        this.dataChunkPriorityQueue = new ConcurrentLinkedQueue<DataChunk>();
    }
    public void addChunk(DataChunk chunk){
        this.dataChunkPriorityQueue.add(chunk);
    }
    public void addAllChunks(List<? extends DataChunk> chunks){
        this.dataChunkPriorityQueue.addAll(chunks);
    }

    @SneakyThrows
    public InputStream condenseListToOneStream(long size){
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] data = new byte[Long.valueOf(size).intValue()];
        ByteBuffer buffer = ByteBuffer.wrap(data);
        List<DataChunk> list = this.dataChunkPriorityQueue.stream().sorted(new DataChunkComparator()).collect(Collectors.toList());
        for(DataChunk currentChunk : list){
            logger.info("Processing chunk {}", currentChunk);
            buffer.put(currentChunk.getData());
            md.update(currentChunk.getData());
        }
        String output = String.format("%032X", new BigInteger(1, md.digest()));
        logger.info(String.valueOf(output));
        this.dataChunkPriorityQueue.clear();
        return new ByteArrayInputStream(buffer.array());
    }

    public void clear(){
        this.dataChunkPriorityQueue.clear();
    }
}