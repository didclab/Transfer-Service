package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class FilePart {
    long partIdx;
    long start;
    long end;
    int size;
    String fileName;
    boolean isLastChunk;

}
