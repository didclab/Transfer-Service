package org.onedatashare.transferservice.odstransferservice.model;

import java.util.Comparator;

public class DataChunkComparator implements Comparator<DataChunk> {
    @Override
    public int compare(DataChunk o1, DataChunk o2) {
        if(o1.getChunkIdx() < o2.getChunkIdx()){
            return -1;
        }else if( o1.getChunkIdx() == o2.getChunkIdx()){
            return 0;
        }else{
            return 1;
        }
    }
}
