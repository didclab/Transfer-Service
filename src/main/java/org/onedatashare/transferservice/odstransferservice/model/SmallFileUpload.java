package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Getter;
import lombok.Setter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.PriorityQueue;

@Getter
@Setter
public class SmallFileUpload {

    private PriorityQueue<DataChunk> dataChunkPriorityQueue;

    public SmallFileUpload(){
        this.dataChunkPriorityQueue = new PriorityQueue<DataChunk>(new DataChunkComparator());
    }

    public void addAllChunks(List<? extends DataChunk> chunks){
        this.dataChunkPriorityQueue.addAll(chunks);
    }

    public InputStream condenseListToOneStream(){
        int totalLength = this.dataChunkPriorityQueue.stream().mapToInt(byteArray -> byteArray.getData().length).sum();
        byte[] combinedBytes = new byte[totalLength];

        int currentIndex = 0;
        for (DataChunk chunk : dataChunkPriorityQueue) {
            byte[] byteArray = chunk.getData();
            System.arraycopy(byteArray, 0, combinedBytes, currentIndex, byteArray.length);
            currentIndex += byteArray.length;
        }

        return new ByteArrayInputStream(combinedBytes);
    }
}
