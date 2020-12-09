package org.onedatashare.transferservice.odstransferservice.model;

import lombok.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

@Data
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class S3DataChunk {
    private List<InputStream> streamList;
    private String fileType;
    private long size;
}
