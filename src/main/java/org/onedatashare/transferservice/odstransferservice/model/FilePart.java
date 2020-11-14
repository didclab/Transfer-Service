package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class FilePart {
    long start;
    long end;
    long size;
    String fileName;
}
