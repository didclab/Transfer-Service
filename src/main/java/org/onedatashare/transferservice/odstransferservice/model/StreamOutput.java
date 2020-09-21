package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import org.onedatashare.transferservice.odstransferservice.service.step.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

@Data
public class StreamOutput {
    Logger logger = LoggerFactory.getLogger(StreamOutput.class);
    public static OutputStream outputStream;

    public static void setOutputStream(OutputStream os) {
        outputStream = os;
    }

    public static OutputStream getOutputStream() {
        return outputStream;
    }
}
