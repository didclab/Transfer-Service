package org.onedatashare.transferservice.odstransferservice.model;

import io.netty.buffer.ByteBuf;
import javassist.bytecode.ByteArray;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.PriorityQueue;

@Getter
@Setter
public class AWSSinglePutRequestMetaData {
    private PriorityQueue<DataChunk> dataChunkPriorityQueue;
    private List<DataChunk> chunkList;

    public AWSSinglePutRequestMetaData(){
        this.dataChunkPriorityQueue = new PriorityQueue<DataChunk>(20, new DataChunkComparator());
    }
    public void addChunk(DataChunk chunk){
        this.dataChunkPriorityQueue.add(chunk);
    }
    public void addAllChunks(List<? extends DataChunk> chunks){
        this.dataChunkPriorityQueue.addAll(chunks);
    }

    public InputStream condenseListToOneStream(long size){
        byte[] data = new byte[Long.valueOf(size).intValue()];
        ByteBuffer buffer = ByteBuffer.wrap(data);
        for(DataChunk chunk : this.dataChunkPriorityQueue){
            buffer.put(chunk.getData());
        }
        return new ByteArrayInputStream(buffer.array());
    }
}
